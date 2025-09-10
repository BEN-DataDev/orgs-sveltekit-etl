import requests
import json
import itertools
from typing import List, Dict, Optional, Any


class ACNCExtractor:
    """
    Extractor class for ACNC Charity Register data that interfaces with the 
    Australian Government Data API. This class is designed to work with the Flask routes system.
    """
    
    def __init__(self):
        self.charity_service = ACNCCharityService()

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Main extraction method called by the Flask routes.
        
        Args:
            **kwargs: Optional search parameters including:
                - town_city: Town or city to search for
                - state: State to search in (can be abbreviation or full name)
                - postcode: Postcode to search for
        
        Returns:
            List of dictionaries containing charity data from ACNC register
        """
        try:
            # Extract search parameters from kwargs
            town_city = kwargs.get('town_city')
            state = kwargs.get('state')
            postcode = kwargs.get('postcode')
            
            # Query the ACNC charity register
            results = self.charity_service.query_charities(
                town_city=town_city,
                state=state,
                postcode=postcode
            )
            
            return results
            
        except Exception as e:
            print(f"Error during ACNC extraction: {str(e)}")
            raise

    def extract_by_location(self, town_city: Optional[str] = None, 
                          state: Optional[str] = None, 
                          postcode: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract charities by location parameters.
        
        Args:
            town_city: Town or city to search for
            state: State to search in
            postcode: Postcode to search for
            
        Returns:
            List of charity records
        """
        return self.extract(town_city=town_city, state=state, postcode=postcode)


class ACNCCharityService:
    """
    Service class for querying the ACNC Charity Register Data API using requests.
    """
    
    CKAN_BASE_URL = 'https://data.gov.au/data/api/3/action/datastore_search'
    RESOURCE_ID = "eb1e6be4-5b13-4feb-b28e-388bf7c26f93"
    PAGE_SIZE = 1000
    
    def __init__(self):
        self.session = requests.Session()

    def query_charities(self, town_city: Optional[str] = None, 
                       state: Optional[str] = None, 
                       postcode: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Queries the ACNC Charity Register Data API based on provided filters.
        
        This function searches for registered charities in Australia using the 
        Australian Charities and Not-for-profits Commission (ACNC) register data.
        It supports filtering by town/city, state, and postcode, and handles 
        various formats and abbreviations for states.
        
        Args:
            town_city (str, optional): The town or city to search for. 
                                     The function will search for both uppercase 
                                     and title case versions.
            state (str, optional): The state to search in. Accepts both 
                                 abbreviations (NSW, VIC, etc.) and full names 
                                 (New South Wales, Victoria, etc.).
            postcode (str, optional): The postcode to search for.
        
        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing charity 
                                records. Each dictionary represents one charity 
                                with various fields like ABN, charity name, 
                                address details, etc. Duplicates based on ABN 
                                are automatically removed.
        
        Note:
            - The function uses pagination to retrieve all matching records
            - It tries different combinations of the provided filters to ensure 
              comprehensive results
            - State abbreviations are automatically expanded to full names where 
              applicable
            - Results are deduplicated based on ABN (Australian Business Number)
        """
        all_found_records = []
        seen_abns = set()

        # --- Prepare filter value options ---
        possible_town_cities = [None]
        if town_city:
            possible_town_cities = list(set([town_city.upper(), town_city.title()]))

        possible_states = [None]
        if state:
            state_mapping = {
                "NSW": "New South Wales", 
                "VIC": "Victoria", 
                "QLD": "Queensland",
                "SA": "South Australia", 
                "WA": "Western Australia", 
                "TAS": "Tasmania",
                "NT": "Northern Territory", 
                "ACT": "Australian Capital Territory"
            }
            possible_states = list(set([state.upper(), state_mapping.get(state.upper(), None)]))
            possible_states = [s for s in possible_states if s is not None]

        possible_postcodes = [None]
        if postcode:
            possible_postcodes = [postcode.upper()]

        # Generate all combinations of filter values
        filter_combinations = itertools.product(
            possible_town_cities, possible_states, possible_postcodes
        )

        # Query each combination
        for tc_val, s_val, pc_val in filter_combinations:
            current_filters = {}
            if tc_val is not None:
                current_filters["Town_City"] = tc_val
            if s_val is not None:
                current_filters["State"] = s_val
            if pc_val is not None:
                current_filters["Postcode"] = pc_val
                
            # Skip if no filters are set
            if not current_filters:
                continue

            print(f"Querying with filters: {current_filters}")
            
            # Paginate through results
            offset = 0
            while True:
                params = {
                    "resource_id": self.RESOURCE_ID,
                    "limit": self.PAGE_SIZE,
                    "offset": offset
                }
                if current_filters:
                    params["filters"] = json.dumps(current_filters)
                
                try:
                    response = self.session.get(self.CKAN_BASE_URL, params=params)
                    response.raise_for_status()
                    data = response.json()
                    
                    if data.get("result", {}).get("records"):
                        records = data["result"]["records"]
                        
                        # Process records and deduplicate by ABN
                        for record in records:
                            abn = record.get('ABN')
                            if abn and abn not in seen_abns:
                                all_found_records.append(record)
                                seen_abns.add(abn)
                        
                        # Check if we've reached the end of results
                        if len(records) < self.PAGE_SIZE:
                            break
                        else:
                            offset += self.PAGE_SIZE
                    else:
                        break
                        
                except requests.RequestException as e:
                    print(f"API error for filters {current_filters}, offset {offset}: {e}")
                    break

        print(f"Total unique charities found: {len(all_found_records)}")
        return all_found_records

    def get_charity_by_abn(self, abn: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific charity by its ABN.
        
        Args:
            abn: The Australian Business Number to search for
            
        Returns:
            Dictionary containing charity data or None if not found
        """
        try:
            params = {
                "resource_id": self.RESOURCE_ID,
                "limit": 1,
                "filters": json.dumps({"ABN": abn})
            }
            
            response = self.session.get(self.CKAN_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            records = data.get("result", {}).get("records", [])
            
            return records[0] if records else None
            
        except requests.RequestException as e:
            print(f"Error fetching charity with ABN {abn}: {e}")
            return None

    def search_by_name(self, charity_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search for charities by name (partial matching).
        
        Args:
            charity_name: The charity name to search for
            limit: Maximum number of results to return
            
        Returns:
            List of matching charity records
        """
        try:
            params = {
                "resource_id": self.RESOURCE_ID,
                "limit": limit,
                "q": charity_name  # This performs a full-text search
            }
            
            response = self.session.get(self.CKAN_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("result", {}).get("records", [])
            
        except requests.RequestException as e:
            print(f"Error searching for charity name '{charity_name}': {e}")
            return []


# Backward compatibility function
def query_acnc_charities(town_city=None, state=None, postcode=None):
    """
    Legacy function for backward compatibility.
    
    Args:
        town_city: Town or city to search for
        state: State to search in
        postcode: Postcode to search for
        
    Returns:
        List of charity records
    """
    service = ACNCCharityService()
    return service.query_charities(town_city=town_city, state=state, postcode=postcode)


# Example usage and testing
if __name__ == "__main__":
    # Test the extractor
    extractor = ACNCExtractor()
    
    # Example: extract data for Sydney, NSW
    sample = extractor.extract(town_city="SYDNEY", state="NSW", postcode="2000")
    print(f"Found {len(sample)} records.")
    
    for i, rec in enumerate(sample[:3]):  # Print only first 3, for brevity
        print(f"\nRecord {i+1}:")
        for key, value in rec.items():
            print(f"  {key}: {value}")
        
    # Test the service directly
    print("\n" + "="*50)
    print("Testing ACNCCharityService directly:")
    
    service = ACNCCharityService()
    
    # Search by name example
    name_results = service.search_by_name("Red Cross", limit=3)
    print(f"\nFound {len(name_results)} charities matching 'Red Cross':")
    for rec in name_results:
        print(f"  - {rec.get('Charity_Name', 'N/A')} (ABN: {rec.get('ABN', 'N/A')})")