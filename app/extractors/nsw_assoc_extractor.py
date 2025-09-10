import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import time
from typing import List, Dict, Optional, Any


class NSWAssocExtractor:
    """
    Extractor class for NSW Association data that interfaces with the NSW Fair Trading
    Association Register. This class is designed to work with the Flask routes system.
    """
    
    BASE_URL = "https://applications.fairtrading.nsw.gov.au/assocregister/"
    DETAILS_URL = "https://applications.fairtrading.nsw.gov.au/assocregister/PublicRegisterDetails.aspx?Organisationid={orgid}"

    def __init__(self):
        self.session = requests.Session()
        self.scraper = NSWAssociationScraper()

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Main extraction method called by the Flask routes.
        
        Args:
            **kwargs: Optional search parameters including:
                - organisation_name: Name to search for
                - organisation_number: Specific organisation number
                - organisation_type: Type of organisation
                - suburb: Suburb to search in
                - postcode: Postcode to search in
                - status: Organisation status
                - delay: Delay between requests (default: 0.5)
        
        Returns:
            List of dictionaries containing organisation data
        """
        try:
            # Extract search parameters from kwargs
            search_params = {
                'organisation_name': kwargs.get('organisation_name'),
                'organisation_number': kwargs.get('organisation_number'),
                'organisation_type': kwargs.get('organisation_type'),
                'suburb': kwargs.get('suburb'),
                'postcode': kwargs.get('postcode'),
                'status': kwargs.get('status'),
                'delay': kwargs.get('delay', 0.5)
            }
            
            # Remove None values
            search_params = {k: v for k, v in search_params.items() if v is not None}
            
            # If no search parameters provided, do a broad search
            if not any(search_params.get(key) for key in ['organisation_name', 'organisation_number', 'organisation_type', 'suburb', 'postcode', 'status']):
                # You might want to set some default search criteria here
                # For now, we'll search for active organisations
                search_params['status'] = 'Active'
            
            results = self.scraper.search_all(**search_params)
            return results
            
        except Exception as e:
            print(f"Error during extraction: {str(e)}")
            raise

    def extract_with_details(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract organisations and fetch detailed information for each.
        
        Args:
            **kwargs: Search parameters (same as extract method)
        
        Returns:
            List of dictionaries with detailed organisation data
        """
        organisations = self.extract(**kwargs)
        detailed_orgs = []
        
        for org in organisations:
            if org.get('organisation_id'):
                details = self.scraper.fetch_org_details(org['organisation_id'])
                if details:
                    # Merge the basic info with detailed info
                    org.update(details)
            detailed_orgs.append(org)
            
        return detailed_orgs


class NSWAssociationScraper:
    """
    Core scraper class for NSW Association Register.
    This class handles the actual web scraping logic.
    """
    
    BASE_URL = "https://applications.fairtrading.nsw.gov.au/assocregister/"
    DETAILS_URL = "https://applications.fairtrading.nsw.gov.au/assocregister/PublicRegisterDetails.aspx?Organisationid={orgid}"

    def __init__(self):
        self.session = requests.Session()

    def _get_form_fields(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract form fields from the page for POST requests."""
        form = soup.find('form', {'id': 'aspnetForm'})
        if form is None:
            raise Exception("Form not found!")
        
        # Convert NavigableString to Tag if necessary
        if isinstance(form, NavigableString):
            form = BeautifulSoup(str(form), 'html.parser')
        
        fields = {}
        
        # Get input fields
        for input_tag in form.find_all('input'):
            name = input_tag.get('name')
            value = input_tag.get('value', '')
            if name:
                fields[name] = value
                
        # Get select fields
        for select_tag in form.find_all('select'):
            name = select_tag.get('name')
            options = select_tag.find_all('option')
            selected = next((opt for opt in options if opt.has_attr('selected')), 
                          options[0] if options else None)
            if name and selected:
                fields[name] = selected.get('value', '')
                
        return fields

    def _parse_results(self, soup: BeautifulSoup) -> List[Dict[str, Optional[str]]]:
        """Parse search results from the results page."""
        results = []
        results_list = soup.find('span', id='ctl00_MainArea_ResultDataList')
        
        if not results_list:
            return results

        # Convert NavigableString to Tag if necessary
        if isinstance(results_list, NavigableString):
            results_list = BeautifulSoup(str(results_list), 'html.parser')
    

        # Each result is a <div class="row"> inside results_list
        for row in results_list.find_all('div', class_='row', recursive=True):
            main_col = row.find('div', class_='col-md-10')
            status_col = row.find('div', class_='col-md-2')
            
            if not main_col or not status_col:
                continue

            # Extract organisation name and ID
            name_a = main_col.find('a')
            name = name_a.get_text(strip=True) if name_a else None

            # Extract Organisation ID from the <a> href attribute
            orgid = None
            if name_a and name_a.has_attr('href'):
                href = name_a['href']
                import re
                match = re.search(r'Organisationid=(\d+)', href)
                if match:
                    orgid = match.group(1)

            # Initialize variables for organisation details
            org_number, org_type, date_registered, date_removed, reg_address = None, None, None, None, None
            
            # Extract organisation details from info rows
            for info_row in main_col.find_all('div', class_='row'):
                for col in info_row.find_all('div'):
                    text = col.get_text(" ", strip=True)
                    if "Organisation Number:" in text:
                        org_number = text.split("Organisation Number:")[-1].strip()
                    if "Organisation Type:" in text:
                        org_type = text.split("Organisation Type:")[-1].strip()
                    if "Date Registered:" in text:
                        date_registered = text.split("Date Registered:")[-1].strip()
                    if "Date Removed:" in text:
                        date_removed = text.split("Date Removed:")[-1].strip()

            # Extract registered address
            reg_addr_div = main_col.find('div', id=lambda x: x and x.endswith('RegisteredAddress'))
            if reg_addr_div:
                reg_address = reg_addr_div.get_text(strip=True).split("Registered Office Address:")[-1].strip()

            # Extract status
            status = None
            if status_col:
                figcaption = status_col.find('figcaption')
                if figcaption:
                    status = figcaption.get_text(strip=True)

            # Add to results if we have a name
            if name:
                results.append({
                    "name": name,
                    "organisation_number": org_number,
                    "organisation_type": org_type,
                    "status": status,
                    "date_registered": date_registered,
                    "date_removed": date_removed,
                    "registered_office_address": reg_address,
                    "organisation_id": orgid
                })
                
        return results

    def _get_next_event_target(self, soup: BeautifulSoup) -> Optional[str]:
        """Find the event target for the next page link."""
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if a_tag.get('id', '').endswith('PageNextLink') and 'javascript:__doPostBack' in href:
                import re
                match = re.search(r"__doPostBack\('([^']+)'", href)
                if match:
                    return match.group(1)
        return None

    def search_all(self, organisation_name: Optional[str] = None, 
                   organisation_number: Optional[str] = None, 
                   organisation_type: Optional[str] = None, 
                   suburb: Optional[str] = None, 
                   postcode: Optional[str] = None, 
                   status: Optional[str] = None, 
                   delay: float = 0.5) -> List[Dict[str, Optional[str]]]:
        """
        Search all pages for organisations matching the given criteria.
        
        Args:
            organisation_name: Name to search for
            organisation_number: Specific organisation number
            organisation_type: Type of organisation
            suburb: Suburb to search in
            postcode: Postcode to search in
            status: Organisation status
            delay: Delay between requests in seconds
            
        Returns:
            List of dictionaries containing organisation data
        """
        all_results = []
        
        # Get initial page and form fields
        response = self.session.get(self.BASE_URL)
        soup = BeautifulSoup(response.text, 'html.parser')
        fields = self._get_form_fields(soup)

        # Set search parameters
        if organisation_name:
            fields['ctl00$MainArea$AdvancedSearchSection$Organisationname'] = organisation_name
        if organisation_number:
            fields['ctl00$MainArea$AdvancedSearchSection$Organisationnumber'] = organisation_number
        if organisation_type:
            fields['ctl00$MainArea$AdvancedSearchSection$Organisationtype'] = organisation_type
        if suburb:
            fields['ctl00$MainArea$AdvancedSearchSection$Suburb'] = suburb
        if postcode:
            fields['ctl00$MainArea$AdvancedSearchSection$Postcode'] = postcode
        if status:
            fields['ctl00$MainArea$AdvancedSearchSection$Organisationstatus'] = status

        # Set search button as event target
        fields['__EVENTTARGET'] = 'ctl00$MainArea$AdvancedSearchSection$AdvancedSearchButton'
        fields['__EVENTARGUMENT'] = ''

        # Perform initial search
        search_response = self.session.post(self.BASE_URL, data=fields)
        search_soup = BeautifulSoup(search_response.text, 'html.parser')
        
        # Parse first page results
        new_results = self._parse_results(search_soup)
        all_results.extend(new_results)
        page_num = 1
        print(f"Fetched page {page_num}: {len(new_results)} results on this page, {len(all_results)} total.")

        # Process additional pages
        while True:
            next_target = self._get_next_event_target(search_soup)
            if not next_target:
                break

            # Get form fields for next page request
            fields = self._get_form_fields(search_soup)
            fields['__EVENTTARGET'] = next_target
            fields['__EVENTARGUMENT'] = ''

            # Add delay to be respectful to the server
            time.sleep(delay)

            # Request next page
            next_response = self.session.post(self.BASE_URL, data=fields)
            next_soup = BeautifulSoup(next_response.text, 'html.parser')
            
            # Parse results
            new_results = self._parse_results(next_soup)
            if not new_results:
                break

            all_results.extend(new_results)
            page_num += 1
            print(f"Fetched page {page_num}: {len(new_results)} results on this page, {len(all_results)} total.")
            search_soup = next_soup

        return all_results

    def fetch_org_details(self, orgid: str) -> Optional[Dict[str, str]]:
        """
        Fetch detailed information for a specific organisation.
        
        Args:
            orgid: The organisation ID to fetch details for
            
        Returns:
            Dictionary containing detailed organisation information or None if failed
        """
        url = self.DETAILS_URL.format(orgid=orgid)
        response = self.session.get(url)
        
        if response.status_code != 200:
            print(f"Failed to fetch details for {orgid}: {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        details = {}
        main_card = soup.find("div", class_="card-body")
        
        # Add type check to ensure main_card is a Tag before calling find_all
        if not main_card or not isinstance(main_card, Tag):
            print("Couldn't find main details area or main_card is not a Tag.")
            return None
            
        # Extract label/value pairs from rows
        for row in main_card.find_all("div", class_="row"):
            # Ensure row is a Tag before calling find_all
            if not isinstance(row, Tag):
                continue
                
            # Find all labels within the row
            labels = row.find_all("span", class_="font-weight-bold")
            for label in labels:
                key = label.get_text(strip=True).replace(":", "").strip()
                # Get the next sibling that contains the value
                value_element = label.next_sibling
                value = None
                
                if isinstance(value_element, Tag):
                    # If the next sibling is a Tag, get its text
                    value = value_element.get_text(strip=True)
                elif value_element:
                    # If it's a NavigableString, use it directly
                    value = str(value_element).strip()
                else:
                    # Fallback: find the next string after the label
                    next_string = label.find_next(string=True)
                    if next_string:
                        value = str(next_string).strip()
                        
                if value:
                    details[key] = value
                    
        return details


# Example usage and testing
if __name__ == "__main__":
    # Test the extractor
    extractor = NSWAssocExtractor()
    
    # Example: extract data for a specific suburb
    results = extractor.extract(suburb="BATLOW")
    
    for i, r in enumerate(results, 1):
        print(f"{i}. {r['name']}")
        print(f"   Number: {r['organisation_number']}")
        print(f"   Type: {r['organisation_type']}")
        print(f"   Status: {r['status']}")
        print(f"   Date Registered: {r['date_registered']}")
        print(f"   Date Removed: {r['date_removed']}")
        print(f"   Registered Office Address: {r['registered_office_address']}")
        print(f"   Organisation ID: {r['organisation_id']}")
        print("---------")
        
        # Example: fetch details for the first organisation
        # if i == 1 and r['organisation_id']:
        #     scraper = NSWAssociationScraper()
        #     details = scraper.fetch_org_details(r['organisation_id'])
        #     print("Details page fields:")
        #     if details:
        #         for k, v in details.items():
        #             print(f"   {k}: {v}")