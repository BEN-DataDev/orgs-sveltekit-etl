from flask import Blueprint, jsonify, Response, request
from app.extractors.abn_extractor import ABNExtractor
from app.extractors.acnc_extractor import ACNCExtractor
from app.extractors.nsw_assoc_extractor import NSWAssocExtractor
from app.transformers.data_transformer import DataTransformer
from app.loaders.supabase_loader import SupabaseLoader
from app.cache.redis_cache import RedisCache
from datetime import datetime
import pytz
from typing import Union, Tuple, Dict, List, Any
import csv
import io
from werkzeug.utils import secure_filename
import concurrent.futures
import threading
from collections import defaultdict

from app.utils.output import write_merged_records_to_csv

api = Blueprint('api', __name__)
cache: RedisCache = RedisCache()  # Type hint for cache
transformer = DataTransformer()
loader = SupabaseLoader()

# Thread lock for Redis operations (though Redis is thread-safe, keeping for consistency)
redis_lock = threading.Lock()

@api.route('/upload/postcodes/<state>', methods=['POST'])
def upload_postcodes(state: str) -> Union[Response, Tuple[Response, int]]:
    """
    Upload a CSV file containing postcodes for a specific state and store as JSON in Redis.
    Expected format: CSV with a 'postcode' column or just comma-separated values.
    
    Args:
        state: State code (e.g., NSW, VIC, QLD)
        
    Returns:
        Response with upload status
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if file.filename is not None and not file.filename.lower().endswith('.csv'):
            return jsonify({'error': 'File must be a CSV'}), 400
        
        # Read and validate the file content
        file_content = file.read().decode('utf-8')
        postcodes = []
        
        # Try to parse as CSV first
        try:
            csv_reader = csv.DictReader(io.StringIO(file_content))
            if csv_reader.fieldnames and 'postcode' in csv_reader.fieldnames:
                for row in csv_reader:
                    postcode = row['postcode'].strip()
                    if postcode and postcode.isdigit():
                        postcodes.append(postcode)
            else:
                # If no 'postcode' column, try first column
                file_content_lines = file_content.strip().split('\n')
                for line in file_content_lines:
                    if line.strip():
                        first_value = line.split(',')[0].strip().strip('"')
                        if first_value.isdigit():
                            postcodes.append(first_value)
        except:
            # If CSV parsing fails, try comma-separated values
            values = file_content.replace('\n', ',').split(',')
            for value in values:
                value = value.strip().strip('"')
                if value.isdigit():
                    postcodes.append(value)
        
        if not postcodes:
            return jsonify({'error': 'No valid postcodes found in file'}), 400
        
        # Remove duplicates while preserving order
        postcodes = list(dict.fromkeys(postcodes))
        
        # Prepare JSON structure
        state_upper = state.upper()
        redis_key = f"postcodes:{state_upper}"
        json_data = {
            'state': state_upper,
            'postcodes': postcodes,
            'upload_timestamp': datetime.now(pytz.UTC).isoformat(),
            'total_postcodes': len(postcodes)
        }
        
        with redis_lock:
            # Store using RedisCache's set method (handles JSON serialization)
            cache.set(redis_key, json_data)
        
        return jsonify({
            'status': 'success',
            'state': state_upper,
            'postcodes_uploaded': len(postcodes),
            'postcodes': postcodes[:10],  # Show first 10 as preview
            'total_postcodes': len(postcodes),
            'redis_key': redis_key
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'status': 'failed'
        }), 500

def load_postcodes_for_state(state: str) -> List[str]:
    """
    Load postcodes from Redis JSON for a given state.
    
    Args:
        state: State code
        
    Returns:
        List of postcodes
    """
    redis_key = f"postcodes:{state.upper()}"
    
    try:
        # Use RedisCache's get method (handles JSON deserialization)
        data = cache.get(redis_key)
        if not data:
            raise KeyError(f"No postcodes found for {state}. Please upload them first.")
        
        # Return sorted postcodes for consistency
        return sorted(data['postcodes'])
    
    except Exception as e:
        raise KeyError(f"Error loading postcodes for {state}: {str(e)}")

def extract_data_for_postcode(source: str, state: str, postcode: str) -> List[Dict[str, Any]]:
    """
    Extract data from a specific source for a given state and postcode.
    
    Args:
        source: Data source (nsw, acnc, abn)
        state: State code
        postcode: Postcode
        
    Returns:
        List of extracted and transformed records
    """
    try:
        if source == 'nsw':
            extractor = NSWAssocExtractor()
            raw_data = extractor.extract(postcode=postcode)
            return transformer.transform_nsw_assoc_data(raw_data)
            
        elif source == 'acnc':
            extractor = ACNCExtractor()
            raw_data = extractor.extract(state=state, postcode=postcode)
            return transformer.transform_acnc_data(raw_data)
            
        elif source == 'abn':
            extractor = ABNExtractor()
            raw_data = extractor.extract(state=state, postcode=postcode)
            return transformer.transform_abn_data(raw_data)
        
        else:
            raise ValueError("Invalid source")
            
    except Exception as e:
        print(f"Error extracting {source} data for {state}/{postcode}: {str(e)}")
        return []

def merge_organization_records(abn_records: List[Dict], acnc_records: List[Dict], nsw_records: List[Dict]) -> Tuple[List[Dict], Dict[str, int]]:
    """
    Merge organization records from different sources based on ABN and name matching.
    
    Args:
        abn_records: Records from ABN source
        acnc_records: Records from ACNC source  
        nsw_records: Records from NSW source
        
    Returns:
        Tuple of (merged_records, statistics)
    """
    merged_records = []
    stats = {
        'total_abn_records': len(abn_records),
        'total_acnc_records': len(acnc_records),
        'total_nsw_records': len(nsw_records),
        'merged_records': 0,
        'abn_only': 0,
        'acnc_only': 0,
        'nsw_only': 0,
        'abn_acnc_matches': 0,
        'acnc_nsw_matches': 0,
        'all_source_matches': 0
    }
    
    # Create dictionaries for efficient lookups
    abn_by_abn = {record['abn']: record for record in abn_records if record.get('abn')}
    acnc_by_abn = {record['abn']: record for record in acnc_records if record.get('abn')}
    
    # For NSW matching, we'll use name matching with ACNC legal names
    nsw_by_name = {}
    for record in nsw_records:
        if record.get('name'):
            # Clean and normalize the name for matching
            clean_name = record['name'].strip().upper()
            nsw_by_name[clean_name] = record
    
    # Track processed records to avoid duplicates
    processed_abns = set()
    processed_nsw_names = set()
    
    # Start with ABN records as the primary source
    for abn_record in abn_records:
        abn = abn_record.get('abn')
        if not abn or abn in processed_abns:
            continue
            
        processed_abns.add(abn)
        
        # Create merged record starting with ABN data
        merged_record = {
            # ABN fields
            'abn': abn_record.get('abn'),
            'is_current': abn_record.get('is_current'),
            'replaced_from': abn_record.get('replaced_from'),
            'entity_status': abn_record.get('entity_status'),
            'effective_from': abn_record.get('effective_from'),
            'effective_to': abn_record.get('effective_to'),
            'entity_type_code': abn_record.get('entity_type_code'),
            'entity_type_description': abn_record.get('entity_type_description'),
            'anc_status': abn_record.get('anc_status'),
            'acnc_status_from': abn_record.get('acnc_status_from'),
            'acnc_status_to': abn_record.get('acnc_status_to'),
            'record_last_updated': abn_record.get('record_last_updated'),
            'gst': abn_record.get('gst'),
            'dgr': abn_record.get('dgr'),
            'main_trading_names': abn_record.get('main_trading_names'),
            'other_trading_names': abn_record.get('other_trading_names'),
            'main_business_physical_address': abn_record.get('main_business_physical_address'),
            'tax_concession_endorsements': abn_record.get('tax_concession_endorsements'),
            
            # ACNC fields (will be filled if match found)
            'legal_name': None,
            'other_organisation_names': None,
            'charity_website': None,
            'date_organisation_established': None,
            'registration_date': None,
            'charity_size': None,
            'number_of_responsible_persons': None,
            'financial_year_end': None,
            'operates_in': None,
            'areas_of_interest': None,
            'acnc_address': None,
            
            # NSW fields (will be filled if match found)
            'nsw_organisation_number': None,
            'nsw_name': None,
            'nsw_organisation_type': None,
            'nsw_status': None,
            'nsw_date_registered': None,
            'nsw_date_removed': None,
            'nsw_registered_office_address': None,
            'nsw_organisation_id': None,
            
            # Metadata
            'sources': ['abn'],
            'updated_at': datetime.now().isoformat()
        }
        
        has_acnc_match = False
        has_nsw_match = False
        
        # Try to match with ACNC by ABN
        if abn in acnc_by_abn:
            acnc_record = acnc_by_abn[abn]
            merged_record.update({
                'legal_name': acnc_record.get('legal_name'),
                'other_organisation_names': acnc_record.get('other_organisation_names'),
                'charity_website': acnc_record.get('charity_wbsite'),  # Note: typo in original transformer
                'date_organisation_established': acnc_record.get('date_organisation_established'),
                'registration_date': acnc_record.get('registration_date'),
                'charity_size': acnc_record.get('charity_size'),
                'number_of_responsible_persons': acnc_record.get('number_of_responsible_persons'),
                'financial_year_end': acnc_record.get('finacial_year_end'),  # Note: typo in original
                'operates_in': acnc_record.get('operates_in'),
                'areas_of_interest': acnc_record.get('areas_of_interest'),
                'acnc_address': acnc_record.get('address')
            })
            merged_record['sources'].append('acnc')
            has_acnc_match = True
            stats['abn_acnc_matches'] += 1
            
            # Now try to match NSW by ACNC legal name
            legal_name = acnc_record.get('legal_name')
            if legal_name:
                clean_legal_name = legal_name.strip().upper()
                if clean_legal_name in nsw_by_name:
                    nsw_record = nsw_by_name[clean_legal_name]
                    merged_record.update({
                        'nsw_organisation_number': nsw_record.get('organisation_number'),
                        'nsw_name': nsw_record.get('name'),
                        'nsw_organisation_type': nsw_record.get('organisation_type'),
                        'nsw_status': nsw_record.get('status'),
                        'nsw_date_registered': nsw_record.get('date_registered'),
                        'nsw_date_removed': nsw_record.get('date_removed'),
                        'nsw_registered_office_address': nsw_record.get('registered_office_address'),
                        'nsw_organisation_id': nsw_record.get('organisation_id')
                    })
                    merged_record['sources'].append('nsw')
                    has_nsw_match = True
                    processed_nsw_names.add(clean_legal_name)
                    stats['acnc_nsw_matches'] += 1
        
        # Update statistics
        if has_acnc_match and has_nsw_match:
            stats['all_source_matches'] += 1
        elif not has_acnc_match and not has_nsw_match:
            stats['abn_only'] += 1
        
        merged_records.append(merged_record)
    
    # Add ACNC-only records (those without ABN matches)
    for acnc_record in acnc_records:
        abn = acnc_record.get('abn')
        if not abn or abn in processed_abns:
            continue
            
        # Create ACNC-only record
        merged_record = {
            # ABN fields (empty)
            'abn': abn,
            'is_current': None,
            'replaced_from': None,
            'entity_status': None,
            'effective_from': None,
            'effective_to': None,
            'entity_type_code': None,
            'entity_type_description': None,
            'anc_status': None,
            'acnc_status_from': None,
            'acnc_status_to': None,
            'record_last_updated': None,
            'gst': None,
            'dgr': None,
            'main_trading_names': None,
            'other_trading_names': None,
            'main_business_physical_address': None,
            'tax_concession_endorsements': None,
            
            # ACNC fields
            'legal_name': acnc_record.get('legal_name'),
            'other_organisation_names': acnc_record.get('other_organisation_names'),
            'charity_website': acnc_record.get('charity_wbsite'),
            'date_organisation_established': acnc_record.get('date_organisation_established'),
            'registration_date': acnc_record.get('registration_date'),
            'charity_size': acnc_record.get('charity_size'),
            'number_of_responsible_persons': acnc_record.get('number_of_responsible_persons'),
            'financial_year_end': acnc_record.get('finacial_year_end'),
            'operates_in': acnc_record.get('operates_in'),
            'areas_of_interest': acnc_record.get('areas_of_interest'),
            'acnc_address': acnc_record.get('address'),
            
            # NSW fields (will be filled if match found)
            'nsw_organisation_number': None,
            'nsw_name': None,
            'nsw_organisation_type': None,
            'nsw_status': None,
            'nsw_date_registered': None,
            'nsw_date_removed': None,
            'nsw_registered_office_address': None,
            'nsw_organisation_id': None,
            
            # Metadata
            'sources': ['acnc'],
            'updated_at': datetime.now().isoformat()
        }
        
        # Try to match NSW by legal name
        legal_name = acnc_record.get('legal_name')
        if legal_name:
            clean_legal_name = legal_name.strip().upper()
            if clean_legal_name in nsw_by_name and clean_legal_name not in processed_nsw_names:
                nsw_record = nsw_by_name[clean_legal_name]
                merged_record.update({
                    'nsw_organisation_number': nsw_record.get('organisation_number'),
                    'nsw_name': nsw_record.get('name'),
                    'nsw_organisation_type': nsw_record.get('organisation_type'),
                    'nsw_status': nsw_record.get('status'),
                    'nsw_date_registered': nsw_record.get('date_registered'),
                    'nsw_date_removed': nsw_record.get('date_removed'),
                    'nsw_registered_office_address': nsw_record.get('registered_office_address'),
                    'nsw_organisation_id': nsw_record.get('organisation_id')
                })
                merged_record['sources'].append('nsw')
                processed_nsw_names.add(clean_legal_name)
                stats['acnc_nsw_matches'] += 1
            else:
                stats['acnc_only'] += 1
        else:
            stats['acnc_only'] += 1
        
        merged_records.append(merged_record)
    
    # Add remaining NSW-only records
    for nsw_record in nsw_records:
        name = nsw_record.get('name')
        if not name:
            continue
            
        clean_name = name.strip().upper()
        if clean_name in processed_nsw_names:
            continue
            
        # Create NSW-only record
        merged_record = {
            # ABN fields (empty)
            'abn': None,
            'is_current': None,
            'replaced_from': None,
            'entity_status': None,
            'effective_from': None,
            'effective_to': None,
            'entity_type_code': None,
            'entity_type_description': None,
            'anc_status': None,
            'acnc_status_from': None,
            'acnc_status_to': None,
            'record_last_updated': None,
            'gst': None,
            'dgr': None,
            'main_trading_names': None,
            'other_trading_names': None,
            'main_business_physical_address': None,
            'tax_concession_endorsements': None,
            
            # ACNC fields (empty)
            'legal_name': None,
            'other_organisation_names': None,
            'charity_website': None,
            'date_organisation_established': None,
            'registration_date': None,
            'charity_size': None,
            'number_of_responsible_persons': None,
            'financial_year_end': None,
            'operates_in': None,
            'areas_of_interest': None,
            'acnc_address': None,
            
            # NSW fields
            'nsw_organisation_number': nsw_record.get('organisation_number'),
            'nsw_name': nsw_record.get('name'),
            'nsw_organisation_type': nsw_record.get('organisation_type'),
            'nsw_status': nsw_record.get('status'),
            'nsw_date_registered': nsw_record.get('date_registered'),
            'nsw_date_removed': nsw_record.get('date_removed'),
            'nsw_registered_office_address': nsw_record.get('registered_office_address'),
            'nsw_organisation_id': nsw_record.get('organisation_id'),
            
            # Metadata
            'sources': ['nsw'],
            'updated_at': datetime.now().isoformat()
        }
        
        stats['nsw_only'] += 1
        merged_records.append(merged_record)
    
    stats['merged_records'] = len(merged_records)
    return merged_records, stats


@api.route('/sync/all/<state>', methods=['POST'])
def sync_all_sources(state: str) -> Union[Response, Tuple[Response, int]]:
    """
    Sync data from all sources (nsw, acnc, abn) for all postcodes in a state.
    Merges records based on ABN and name matching.
    
    Args:
        state: State code (e.g., NSW, VIC, QLD)
        
    Returns:
        Response with merged records and statistics
    """
    try:
        state_upper = state.upper()
        
        # Check cache first
        cache_key = f'sync_all_{state_upper}'
        cached_data = cache.get(cache_key)
        if cached_data:
            return jsonify(cached_data)
        
        # Load postcodes for the state
        postcodes = load_postcodes_for_state(state_upper)
        
        if not postcodes:
            return jsonify({'error': f'No postcodes found for {state_upper}'}), 400
        
        print(f"Starting bulk sync for {state_upper} with {len(postcodes)} postcodes")
        
        # Collect all records from all sources and postcodes
        all_abn_records = []
        all_acnc_records = []
        all_nsw_records = []
        
        postcode_stats = {
            'total_postcodes': len(postcodes),
            'processed_postcodes': 0,
            'failed_postcodes': [],
            'by_postcode': {}
        }
        
        # Use ThreadPoolExecutor for concurrent processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all extraction tasks
            futures = {}
            for postcode in postcodes:
                for source in ['abn', 'acnc', 'nsw']:
                    future = executor.submit(extract_data_for_postcode, source, state_upper, postcode)
                    futures[future] = (source, postcode)
            
            # Collect results
            for future in concurrent.futures.as_completed(futures):
                source, postcode = futures[future]
                try:
                    records = future.result()
                    
                    # Initialize postcode stats if needed
                    if postcode not in postcode_stats['by_postcode']:
                        postcode_stats['by_postcode'][postcode] = {
                            'abn': 0, 'acnc': 0, 'nsw': 0, 'total': 0
                        }
                    
                    # Add records to appropriate collections
                    if source == 'abn':
                        all_abn_records.extend(records)
                        postcode_stats['by_postcode'][postcode]['abn'] = len(records)
                    elif source == 'acnc':
                        all_acnc_records.extend(records)
                        postcode_stats['by_postcode'][postcode]['acnc'] = len(records)
                    elif source == 'nsw':
                        all_nsw_records.extend(records)
                        postcode_stats['by_postcode'][postcode]['nsw'] = len(records)
                    
                    postcode_stats['by_postcode'][postcode]['total'] = (
                        postcode_stats['by_postcode'][postcode]['abn'] +
                        postcode_stats['by_postcode'][postcode]['acnc'] +
                        postcode_stats['by_postcode'][postcode]['nsw']
                    )
                    
                    print(f"Completed {source} extraction for {postcode}: {len(records)} records")
                    
                except Exception as e:
                    print(f"Failed to extract {source} data for {postcode}: {str(e)}")
                    if postcode not in postcode_stats['failed_postcodes']:
                        postcode_stats['failed_postcodes'].append(postcode)
        
        # Count successfully processed postcodes
        postcode_stats['processed_postcodes'] = len([
            p for p in postcodes 
            if p in postcode_stats['by_postcode'] and postcode_stats['by_postcode'][p]['total'] > 0
        ])
        
        print(f"Extraction complete. Found {len(all_abn_records)} ABN, {len(all_acnc_records)} ACNC, {len(all_nsw_records)} NSW records")
        
        # Merge all records
        merged_records, merge_stats = merge_organization_records(
            all_abn_records, all_acnc_records, all_nsw_records
        )
        
        print(f"Merging complete. Created {len(merged_records)} merged records")
        
        # Load merged records to Supabase
        if merged_records:
            write_merged_records_to_csv(merged_records, f'merged_records_{state_upper}.csv')
            loader_result = loader.upsert_organizations(merged_records)
            print(f"Loaded {len(merged_records)} records to Supabase")
        else:
            loader_result = "No records to load"
        
        # Prepare response
        response_data = {
            'status': 'success',
            'state': state_upper,
            'postcode_stats': postcode_stats,
            'merge_stats': merge_stats,
            'merged_records_count': len(merged_records),
            'loader_result': loader_result,
            'merged_records': merged_records[:100],  # Return first 100 for preview
            'total_merged_records': len(merged_records),
            'processing_time': datetime.now().isoformat()
        }
        
        # Cache the result (cache for 1 hour due to bulk nature)
        cache.set(cache_key, response_data, ttl=3600)
        
        return jsonify(response_data)
        
    except KeyError as e:
        return jsonify({
            'error': str(e),
            'suggestion': f'Upload postcodes for {state} using /upload/postcodes/{state}',
            'status': 'failed'
        }), 404
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'state': state,
            'status': 'failed'
        }), 500


@api.route('/postcodes/<state>', methods=['GET'])
def get_postcodes(state: str) -> Union[Response, Tuple[Response, int]]:
    """
    Get the list of postcodes for a specific state from Redis.
    
    Args:
        state: State code
        
    Returns:
        Response with postcodes list
    """
    try:
        postcodes = load_postcodes_for_state(state.upper())
        return jsonify({
            'status': 'success',
            'state': state.upper(),
            'postcodes': postcodes,
            'total_postcodes': len(postcodes)
        })
    except KeyError as e:
        return jsonify({
            'error': str(e),
            'status': 'failed'
        }), 404
    except Exception as e:
        return jsonify({
            'error': str(e),
            'status': 'failed'
        }), 500


# Keep all your existing routes below...
@api.route('/sync/<source>', methods=['POST'])
def sync_data(source: str) -> Union[Response, Tuple[Response, int]]:
    """
    Sync data for a specific source.
    
    Args:
        source: The source to sync data for (nsw, acnc, abn)
        
    Returns:
        Either a Response object or a tuple of (Response, status_code)
    """
    try:
        # Check cache first
        cached_data = cache.get(f'sync_{source}')
        if cached_data:
            return jsonify(cached_data)

        # Extract data based on source
        if source == 'nsw':
            extractor = NSWAssocExtractor()
            raw_data = extractor.extract()
            # Alternative with parameters:
            # raw_data = extractor.extract(suburb="SYDNEY", status="Active")
            transformed_data = transformer.transform_nsw_assoc_data(raw_data)
            
        elif source == 'acnc':
            extractor = ACNCExtractor()
            raw_data = extractor.extract()
            print(f"Raw ACNC data (length: {len(raw_data)}): {raw_data[:3]}")  # Log first 3 records
            transformed_data = transformer.transform_acnc_data(raw_data)
            print(f"Transformed ACNC data (length: {len(transformed_data)}): {transformed_data[:3]}")  # Log first 3
            if not transformed_data:
                return jsonify({
                    'status': 'failed',
                    'source': source,
                    'error': 'No data to upsert after transformation'
                }), 400
            result = loader.upsert_organizations(transformed_data)
            cache.set(f'sync_{source}', transformed_data)
            return jsonify({
                'status': 'success',
                'source': source,
                'records_processed': len(transformed_data),
                'data': result
            })
            
        elif source == 'abn':
            extractor = ABNExtractor()
            raw_data = extractor.extract()
            # Alternative with parameters:
            # raw_data = extractor.extract(state="NSW", postcode="2000", max_abns=100)
            transformed_data = transformer.transform_abn_data(raw_data)
            
        else:
            return jsonify({'error': 'Invalid source. Valid sources are: nsw, acnc, abn'}), 400

        # Load to Supabase
        result = loader.upsert_organizations(transformed_data)

        # Cache the result
        cache.set(f'sync_{source}', transformed_data)

        return jsonify({
            'status': 'success',
            'source': source,
            'records_processed': len(transformed_data),
            'data': result
        })

    except Exception as e:
        return jsonify({
            'error': str(e),
            'source': source,
            'status': 'failed'
        }), 500
    

@api.route('/sync/<source>/<state>', methods=['POST'])
def sync_data_with_state(source: str, state: str) -> Union[Response, Tuple[Response, int]]:
    """
    Sync data for a specific source with state parameter.
    Useful for ABN searches that require state parameter.
    
    Args:
        source: The source to sync data for (nsw, acnc, abn)
        state: State code (e.g., NSW, VIC, QLD)
        
    Returns:
        Either a Response object or a tuple of (Response, status_code)
    """
    try:
        # Check cache first
        cache_key = f'sync_{source}_{state}'
        cached_data = cache.get(cache_key)
        if cached_data:
            return jsonify(cached_data)

        # Extract data based on source with state parameter
        if source == 'nsw':
            extractor = NSWAssocExtractor()
            # For NSW, state might be used differently - you can customize this
            raw_data = extractor.extract()
            transformed_data = transformer.transform_nsw_assoc_data(raw_data)
            
        elif source == 'acnc':
            extractor = ACNCExtractor()
            raw_data = extractor.extract(state=state)
            transformed_data = transformer.transform_acnc_data(raw_data)
            
        elif source == 'abn':
            extractor = ABNExtractor()
            # For ABN, we need both state and postcode. Default postcode for major cities
            state_postcodes = {
                'NSW': '2000',  # Sydney
                'VIC': '3000',  # Melbourne
                'QLD': '4000',  # Brisbane
                'SA': '5000',   # Adelaide
                'WA': '6000',   # Perth
                'TAS': '7000',  # Hobart
                'NT': '0800',   # Darwin
                'ACT': '2600'   # Canberra
            }
            postcode = state_postcodes.get(state.upper(), '2000')
            raw_data = extractor.extract(state=state, postcode=postcode)
            transformed_data = transformer.transform_abn_data(raw_data)
            
        else:
            return jsonify({'error': 'Invalid source. Valid sources are: nsw, acnc, abn'}), 400

        # Load to Supabase
        result = loader.upsert_organizations(transformed_data)

        # Cache the result
        cache.set(cache_key, transformed_data)

        return jsonify({
            'status': 'success',
            'source': source,
            'state': state,
            'records_processed': len(transformed_data),
            'data': result
        })

    except Exception as e:
        return jsonify({
            'error': str(e),
            'source': source,
            'state': state,
            'status': 'failed'
        }), 500


@api.route('/sync/<source>/<state>/<postcode>', methods=['POST'])
def sync_data_with_location(source: str, state: str, postcode: str) -> Union[Response, Tuple[Response, int]]:
    """
    Sync data for a specific source with state and postcode parameters.
    Provides the most granular control over location-based searches.
    
    Args:
        source: The source to sync data for (nsw, acnc, abn)
        state: State code (e.g., NSW, VIC, QLD)
        postcode: Postcode to search in
        
    Returns:
        Either a Response object or a tuple of (Response, status_code)
    """
    try:
        # Check cache first
        cache_key = f'sync_{source}_{state}_{postcode}'
        cached_data = cache.get(cache_key)
        if cached_data:
            return jsonify(cached_data)

        # Extract data based on source with full location parameters
        if source == 'nsw':
            extractor = NSWAssocExtractor()
            # For NSW, we can search by postcode
            raw_data = extractor.extract(postcode=postcode)
            transformed_data = transformer.transform_nsw_assoc_data(raw_data)
            
        elif source == 'acnc':
            extractor = ACNCExtractor()
            raw_data = extractor.extract(state=state, postcode=postcode)
            transformed_data = transformer.transform_acnc_data(raw_data)
            
        elif source == 'abn':
            extractor = ABNExtractor()
            raw_data = extractor.extract(state=state, postcode=postcode)
            transformed_data = transformer.transform_abn_data(raw_data)
            
        else:
            return jsonify({'error': 'Invalid source. Valid sources are: nsw, acnc, abn'}), 400

        # Load to Supabase
        result = loader.upsert_organizations(transformed_data)

        # Cache the result
        cache.set(cache_key, transformed_data)

        return jsonify({
            'status': 'success',
            'source': source,
            'state': state,
            'postcode': postcode,
            'records_processed': len(transformed_data),
            'data': result
        })

    except Exception as e:
        return jsonify({
            'error': str(e),
            'source': source,
            'state': state,
            'postcode': postcode,
            'status': 'failed'
        }), 500


@api.route('/lookup/abn/<abn_number>', methods=['GET'])
def lookup_abn(abn_number: str) -> Union[Response, Tuple[Response, int]]:
    """
    Look up details for a specific ABN.
    
    Args:
        abn_number: The ABN to look up
        
    Returns:
        Either a Response object or a tuple of (Response, status_code)
    """
    try:
        # Check cache first
        cache_key = f'abn_lookup_{abn_number}'
        cached_data = cache.get(cache_key)
        if cached_data:
            return jsonify(cached_data)

        extractor = ABNExtractor()
        result = extractor.lookup_abn(abn_number)
        
        if not result:
            return jsonify({'error': 'ABN not found or invalid'}), 404

        # Cache the result
        cache.set(cache_key, result)

        return jsonify({
            'status': 'success',
            'abn': abn_number,
            'data': result
        })

    except Exception as e:
        return jsonify({
            'error': str(e),
            'abn': abn_number,
            'status': 'failed'
        }), 500


@api.route('/sources', methods=['GET'])
def get_available_sources() -> Response:
    """
    Get list of available data sources and their descriptions.
    
    Returns:
        Response object with available sources information
    """
    sources = {
        'nsw': {
            'name': 'NSW Fair Trading Association Register',
            'description': 'NSW incorporated associations and cooperatives',
            'parameters': ['organisation_name', 'organisation_number', 'organisation_type', 'suburb', 'postcode', 'status'],
            'example_endpoints': [
                '/sync/nsw',
                '/sync/nsw/NSW/2000'
            ]
        },
        'acnc': {
            'name': 'Australian Charities and Not-for-profits Commission',
            'description': 'Registered Australian charities',
            'parameters': ['town_city', 'state', 'postcode'],
            'example_endpoints': [
                '/sync/acnc',
                '/sync/acnc/NSW',
                '/sync/acnc/NSW/2000'
            ]
        },
        'abn': {
            'name': 'Australian Business Register',
            'description': 'Australian businesses and charities with ABNs',
            'parameters': ['state', 'postcode', 'max_abns'],
            'example_endpoints': [
                '/sync/abn',
                '/sync/abn/NSW',
                '/sync/abn/NSW/2000',
                '/lookup/abn/12345678901'
            ]
        },
        'bulk': {
            'name': 'Bulk Processing',
            'description': 'Process multiple postcodes and merge data from all sources',
            'parameters': ['state'],
            'example_endpoints': [
                '/upload/postcodes/NSW',
                '/sync/all/NSW',
                '/postcodes/NSW'
            ]
        }
    }
    
    return jsonify({
        'status': 'success',
        'available_sources': sources,
        'total_sources': len(sources)
    })

@api.route('/health', methods=['GET'])
def health_check() -> Response:
    """
    Health check endpoint for Render
    """
    utc_now = datetime.now(pytz.UTC)

    return jsonify({
        'status': 'healthy',
        'timestamp': utc_now.strftime('%Y-%m-%d %H:%M:%S'),
        'service': 'orgs-sveltekit-etl',
        'user': 'BEN-DataDev',
        'available_sources': ['nsw', 'acnc', 'abn', 'bulk'],
        'bulk_features': {
            'postcode_upload': True,
            'multi_source_merge': True,
            'concurrent_processing': True,
            'redis_caching': True
        }
    })