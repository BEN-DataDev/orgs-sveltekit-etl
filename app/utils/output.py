import csv
from datetime import datetime

def write_merged_records_to_csv(merged_records, filename='merged_records.csv'):
    """
    Write merged records to a CSV file.
    
    Args:
        merged_records (List[Dict]): List of merged records
        filename (str): Name of the output CSV file
    """
    # Define the field names
    fieldnames = [
        'abn', 'is_current', 'replaced_from', 'entity_status', 'effective_from', 'effective_to',
        'entity_type_code', 'entity_type_description', 'anc_status', 'acnc_status_from', 'acnc_status_to',
        'record_last_updated', 'gst', 'dgr', 'main_trading_names', 'other_trading_names',
        'main_business_physical_address', 'tax_concession_endorsements',
        'legal_name', 'other_organisation_names', 'charity_website', 'date_organisation_established',
        'registration_date', 'charity_size', 'number_of_responsible_persons', 'financial_year_end',
        'operates_in', 'areas_of_interest', 'acnc_address',
        'nsw_organisation_number', 'nsw_name', 'nsw_organisation_type', 'nsw_status',
        'nsw_date_registered', 'nsw_date_removed', 'nsw_registered_office_address', 'nsw_organisation_id',
        'sources', 'updated_at'
    ]

    # Write the merged records to a CSV file
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write the header
        writer.writeheader()
        
        # Write the merged records
        for record in merged_records:
            writer.writerow(record)

    print(f"Successfully wrote {len(merged_records)} merged records to {filename}")
