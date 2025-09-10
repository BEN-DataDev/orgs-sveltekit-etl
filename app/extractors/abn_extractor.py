import os
import time
import logging
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv
from requests import Session
from requests.exceptions import RequestException
import zeep
from zeep.transports import Transport
import xml.etree.ElementTree as ET

load_dotenv()
ABN_GUID: str = os.getenv("PRIVATE_ABN_SEARCH_GUID", "")
if not ABN_GUID:
    raise ValueError("PRIVATE_ABN_SEARCH_GUID environment variable is not set.")

NAMESPACE = {'ns': 'http://abr.business.gov.au/ABRXMLSearch/'}


class ABNExtractor:
    """
    Extractor class for Australian Business Register (ABR) data that interfaces with the 
    ABR XML Search API. This class is designed to work with the Flask routes system.
    """
    
    def __init__(self):
        self.abr_service = ABRService()

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Main extraction method called by the Flask routes.
        
        Args:
            **kwargs: Optional search parameters including:
                - state: State to search in (required for charity search)
                - postcode: Postcode to search for (required for charity search)
                - max_abns: Maximum number of ABNs to process (optional)
        
        Returns:
            List of dictionaries containing charity/business data from ABR register
        """
        try:
            # Extract search parameters from kwargs
            state = kwargs.get('state')
            postcode = kwargs.get('postcode')
            max_abns = kwargs.get('max_abns')
            
            # For now, we'll default to charity search if no specific method is requested
            # You can extend this to support other ABR search types in the future
            if not state or not postcode:
                # If no state/postcode provided, you might want to set defaults
                # or raise an error. For now, we'll use some common defaults
                state = state or 'NSW'
                postcode = postcode or '2000'
            
            results = self.abr_service.search_charities(
                state=state,
                postcode=postcode,
                max_abns=max_abns
            )
            
            return results
            
        except Exception as e:
            print(f"Error during ABN extraction: {str(e)}")
            raise

    def extract_charities(self, state: str, postcode: str, 
                         max_abns: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Extract registered charities by state and postcode.
        
        Args:
            state: State code (e.g., 'NSW', 'VIC')
            postcode: Postcode to search in
            max_abns: Maximum number of ABNs to process
            
        Returns:
            List of charity records
        """
        return self.extract(state=state, postcode=postcode, max_abns=max_abns)

    def lookup_abn(self, abn: str) -> Optional[Dict[str, Any]]:
        """
        Look up details for a specific ABN.
        
        Args:
            abn: The Australian Business Number to look up
            
        Returns:
            Dictionary containing business details or None if not found
        """
        try:
            client = ABRClient(ABN_GUID)
            parsed_details = client._lookup_abn_details(abn)
            
            if not parsed_details:
                return None
                
            be = extract_business_entity(parsed_details)
            return format_record(be) if be else None
            
        except Exception as e:
            print(f"Error looking up ABN {abn}: {str(e)}")
            return None


class ABRService:
    """
    Service class for querying the Australian Business Register (ABR).
    """
    
    def __init__(self):
        self.client = ABRClient(ABN_GUID)

    def search_charities(self, state: str, postcode: str, 
                        max_abns: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Search for registered charities in a specific state and postcode.
        
        Args:
            state: State code to search in
            postcode: Postcode to search for
            max_abns: Maximum number of ABNs to process
            
        Returns:
            List of charity records
        """
        try:
            return self.client.search_charities(
                postcode=postcode,
                state=state,
                max_abns=max_abns
            )
        except Exception as e:
            print(f"Error searching charities: {str(e)}")
            raise
        finally:
            if hasattr(self.client, 'session'):
                self.client.session.close()


class ABRClient:
    def __init__(self, guid: str):
        self.guid = guid
        self.session = Session()
        self.transport = CustomTransport(session=self.session)
        self.client = zeep.Client(
            'https://abr.business.gov.au/ABRXMLSearch/AbrXmlSearch.asmx?WSDL',
            transport=self.transport
        )
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self._check_maintenance()

    def _check_maintenance(self):
        """Check if ABR service is under maintenance."""
        windows = [
            {'start': '2025-08-02 21:00:00+10:00', 'end': '2025-08-03 14:00:00+10:00'},
            {'start': '2025-08-09 21:00:00+10:00', 'end': '2025-08-10 10:00:00+10:00'}
        ]
        now = datetime.now(timezone(timedelta(hours=10)))
        for w in windows:
            start = datetime.strptime(w['start'], '%Y-%m-%d %H:%M:%S%z')
            end = datetime.strptime(w['end'], '%Y-%m-%d %H:%M:%S%z')
            if start <= now <= end:
                raise RuntimeError(f"ABR Service under maintenance until {end.strftime('%Y-%m-%d %H:%M AEST')}")

    def search_charities(self, postcode: str, state: str, max_abns: Optional[int] = None) -> List[Dict]:
        """
        Search for charities in a specific postcode and state.
        
        Args:
            postcode: Postcode to search in
            state: State to search in
            max_abns: Maximum number of ABNs to process
            
        Returns:
            List of charity records
        """
        search_params = {
            'postcode': postcode,
            'state': '',
            'charityTypeCode': '',
            'concessionTypeCode': '',  # blank for all charity types
            'authenticationGuid': self.guid
        }
        self.logger.info(f"Searching for charities in postcode {postcode}")

        abns = self._call_search_by_charity(search_params, max_abns)
        charities = []

        for abn in abns:
            parsed_details = self._lookup_abn_details(abn)
            if not parsed_details:
                continue

            be = extract_business_entity(parsed_details)

            # Only include those whose main location matches search
            main_addr = be.get("mainBusinessPhysicalAddress")
            postcode_val = None
            state_code = None
            if main_addr:
                postcode_val = main_addr.get("postcode")
                state_code = (main_addr.get("stateCode") or '').upper()
            if state_code == state.upper() and postcode_val == postcode:
                charities.append(format_record(be))
            time.sleep(0.4)  # Rate limiting
            
        self.logger.info(f"Returning {len(charities)} charity results")
        return charities

    def _call_search_by_charity(self, params: Dict, limit: Optional[int]) -> List[str]:
        """Call the SearchByCharity API endpoint."""
        max_retries = 3
        content = None        
        for attempt in range(max_retries):
            try:
                self.client.service.SearchByCharity(**params)
                if not self.transport.last_response:
                    raise RuntimeError("No response from SearchByCharity")
                content = self.transport.last_response.content
                break
            except RequestException as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)

        if content is None:
            raise RuntimeError("Failed to get response content after all retries")

        if isinstance(content, bytes):
            result = content.decode('utf-8')
        else:
            result = content
            
        root = ET.fromstring(result)
        abn_list = root.findall('.//ns:abn', namespaces=NAMESPACE)
        abns = [abn.text for abn in abn_list if abn.text]
        if limit is not None:
            abns = abns[:limit]
        return abns

    def _lookup_abn_details(self, abn: str) -> Optional[Any]:
        """
        Look up detailed information for a specific ABN.
        
        Args:
            abn: The ABN to look up
            
        Returns:
            Parsed XML response as nested dict or None if failed
        """
        max_retries = 3
        params = dict(
            searchString=abn,
            includeHistoricalDetails='N',
            authenticationGuid=self.guid
        )
        res = None

        for attempt in range(max_retries):
            try:
                self.client.service.SearchByABNv201408(**params)
                if not self.transport.last_response:
                    raise RuntimeError(f"No response for ABN {abn}")
                res = self.transport.last_response.content
                break
            except RequestException as e:
                if attempt == max_retries - 1:
                    logging.error(f"Failed SearchByABNv201408 for ABN {abn} after {max_retries} attempts: {e}")
                    return None
                time.sleep(2 ** attempt)

        if res is None:
            logging.error(f"No response content received for ABN {abn}")
            return None

        if isinstance(res, bytes):
            res = res.decode('utf-8')

        root = ET.fromstring(res)
        # Extract the entire response
        return etree_to_dict(root)


class CustomTransport(Transport):
    """Custom transport to capture the last response."""
    
    def __init__(self, session=None):
        super().__init__(session=session)
        self.last_response = None
        
    def post(self, address, message, headers):
        response = super().post(address, message, headers)
        self.last_response = response
        return response


def etree_to_dict(elem) -> Any:
    """
    Recursively convert an xml.etree.ElementTree.Element into a dict or a text value.
    
    Args:
        elem: XML element to convert
        
    Returns:
        Dictionary representation of the XML element
    """
    d = {}
    children = list(elem)
    if children:
        child_dict = {}
        for child in children:
            tag = child.tag
            ns_idx = tag.find('}')
            if ns_idx != -1:
                tag = tag[ns_idx + 1:]
            child_value = etree_to_dict(child)
            if tag in child_dict:
                if isinstance(child_dict[tag], list):
                    child_dict[tag].append(child_value)
                else:
                    child_dict[tag] = [child_dict[tag], child_value]
            else:
                child_dict[tag] = child_value
        d.update(child_dict)
    text = (elem.text or '').strip()
    if text and not children:
        return text
    elif text:
        d['value'] = text
    return d


def extract_business_entity(details: dict) -> dict:
    """
    Extract the businessEntity201408 dictionary from parsed ABR XML.
    
    Args:
        details: Parsed XML response as dictionary
        
    Returns:
        Business entity data dictionary
    """
    response = (
        details.get("Body", {})
        .get("SearchByABNv201408Response", {})
        .get("ABRPayloadSearchResults", {})
        .get("response", {})
    )
    return response.get("businessEntity201408", {})


def format_record(be: dict) -> dict:
    """
    Format a businessEntity201408 dict into the requested flat record structure.
    
    Args:
        be: Business entity dictionary from ABR response
        
    Returns:
        Formatted record dictionary
    """
    def get_path(node, *path):
        curr = node
        for key in path:
            if isinstance(curr, list):
                curr = curr[0] if curr else None
            if not isinstance(curr, dict):
                return None
            curr = curr.get(key)
        if isinstance(curr, dict) or isinstance(curr, list):
            return curr
        return curr

    abn = get_path(be, "ABN", "identifierValue")
    is_current = get_path(be, "ABN", "isCurrentIndicator")
    replaced_from = get_path(be, "ABN", "replacedFrom")
    entity_status = get_path(be, "entityStatus", "entityStatusCode")
    effective_from = get_path(be, "entityStatus", "effectiveFrom")
    effective_to = get_path(be, "entityStatus", "effectiveTo")
    entity_type_code = get_path(be, "entityType", "entityTypeCode")
    entity_type_description = get_path(be, "entityType", "entityDescription")
    acnc_status = get_path(be, "ACNCRegistration", "status")
    acnc_status_from = get_path(be, "ACNCRegistration", "effectiveFrom")
    acnc_status_to = get_path(be, "ACNCRegistration", "effectiveTo")
    record_last_updated = be.get("recordLastUpdatedDate")

    # JSON serialize complex objects
    gst = json.dumps(be.get("goodsAndServicesTax")) if be.get("goodsAndServicesTax") is not None else None
    dgr = json.dumps(be.get("dgrEndorsement")) if be.get("dgrEndorsement") is not None else None
    main_trading_names = json.dumps(be.get("mainTradingName")) if be.get("mainTradingName") is not None else None
    other_trading_names = json.dumps(be.get("otherTradingName")) if be.get("otherTradingName") is not None else None
    main_business_physical_address = json.dumps(be.get("mainBusinessPhysicalAddress")) if be.get("mainBusinessPhysicalAddress") is not None else None
    tax_concession_endorsements = json.dumps(be.get("taxConcessionCharityEndorsement")) if be.get("taxConcessionCharityEndorsement") is not None else None

    return {
        "abn": abn,
        "isCurrent": is_current,
        "replacedFrom": replaced_from,
        "entityStatus": entity_status,
        "effectiveFrom": effective_from,
        "effectiveTo": effective_to,
        "entityTypeCode": entity_type_code,
        "entityDescription": entity_type_description,
        "acnc_status": acnc_status,
        "acnc_status_from": acnc_status_from,
        "acnc_status_to": acnc_status_to,
        "record_last_updated": record_last_updated,
        "gst": gst,
        "dgr": dgr,
        "main_trading_names": main_trading_names,
        "other_trading_names": other_trading_names,
        "main_business_physical_address": main_business_physical_address,
        "tax_concession_endorsements": tax_concession_endorsements,
    }


# Backward compatibility function
def query_abn_register(state: str, postcode: str, max_abns: Optional[int] = None) -> List[Dict]:
    """
    Legacy function for backward compatibility.
    
    Args:
        state: State to search in
        postcode: Postcode to search for
        max_abns: Maximum number of ABNs to process
        
    Returns:
        List of charity records
    """
    service = ABRService()
    return service.search_charities(state=state, postcode=postcode, max_abns=max_abns)


def main():
    """Example usage of the ABN extractor."""
    client = ABRClient(ABN_GUID)
    try:
        charities = client.search_charities(postcode="2730", state="NSW")
        print(f"\nFound {len(charities)} charities:\n")
        for charity in charities:
            for k, v in charity.items():
                print(f"{k}: {v}")
            print("------")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if hasattr(client, 'session'):
            client.session.close()


# Example usage and testing
if __name__ == "__main__":
    # Test the extractor
    extractor = ABNExtractor()
    
    # Example: extract data for NSW postcode 2000
    print("Testing ABNExtractor:")
    sample = extractor.extract(state="NSW", postcode="2000", max_abns=5)
    print(f"Found {len(sample)} records.")
    
    for i, rec in enumerate(sample[:3]):  # Print only first 3, for brevity
        print(f"\nRecord {i+1}:")
        print(f"  ABN: {rec.get('abn', 'N/A')}")
        print(f"  Entity Description: {rec.get('entityDescription', 'N/A')}")
        print(f"  Status: {rec.get('entityStatus', 'N/A')}")
        print(f"  ACNC Status: {rec.get('acnc_status', 'N/A')}")
        
    # Test direct service usage
    print("\n" + "="*50)
    print("Testing ABRService directly:")
    
    service = ABRService()
    direct_results = service.search_charities(state="NSW", postcode="2000", max_abns=3)
    print(f"Direct service returned {len(direct_results)} results")