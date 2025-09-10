from datetime import datetime

class DataTransformer:
    @staticmethod
    def transform_abn_data(raw_data):
        transformed = []
        for item in raw_data:
            transformed.append({
                'abn': item.get('abn'),
                'is_current': item.get('isCurrent'),
                'replaced_from': item.get('replacedFrom'),
                'entity_status': item.get('entityStatus'),
                'effective_from': item.get('effectiveFrom'),
                'effective_to': item.get('effectiveTo'),
                'entity_type_code': item.get('entityTypeCode'),
                'entity_type_description': item.get('entityTypeDescription'),
                'anc_status': item.get('anc_status'),
                'acnc_status_from': item.get('acnc_status_from'), 
                'acnc_status_to': item.get('acnc_status_to'), 
                'record_last_updated': item.get('record_last_updated'), 
                'gst': item.get('gst'),
                'dgr': item.get('dgr'),
                'main_trading_names': item.get('main_trading_names'),
                'other_trading_names': item.get('other_trading_names'),
                'main_business_physical_address': item.get('main_business_physical_address', {}),
                'tax_concession_endorsements': item.get('tax_concession_endorsements', []),
                'updated_at': datetime.now().isoformat()
                # Add more fields as needed
            })
        return transformed

    @staticmethod
    def transform_acnc_data(raw_data):
        transformed = []
        for item in raw_data:
            transformed.append({
                'abn': item.get('ABN'),
                'legal_name': item.get('Charity_Legal_Name'),
                'other_organisation_names': item.get('Other_Organisation_Names'),
                'address': {
                    'street': item.get('Address_Type'),
                    'street': item.get('Address_Line_1'),
                    'street': item.get('Address_Line_2'),
                    'street': item.get('Address_Line_3'),
                    'city': item.get('Town_City'),
                    'state': item.get('State'),
                    'postcode': item.get('Postcode'),
                    'country': item.get('Country')
                },
                'charity_wbsite': item.get('Charity_Website'),
                'date_organisation_established': item.get('Date_Organisation_Established'),
                'registration_date': item.get('Registration_Date'),
                'charity_size': item.get('Charity_Size'),
                'number_of_responsible_persons': item.get('Number_of_Responsible_Persons'),
                'finacial_year_end': item.get('Financial_Year_End'),
                'operates_in': {
                    'act': item.get('Operates_in_ACT'),
                    'nsw': item.get('Operates_in_NSW'),
                    'nt': item.get('Operates_in_NT'),
                    'qld': item.get('Operates_in_QLD'),
                    'sa': item.get('Operates_in_SA'),
                    'tas': item.get('Operates_in_TAS'),
                    'vic': item.get('Operates_in_VIC'),
                    'wa': item.get('Operates_in_WA'),
                    'countries': item.get('Operates_in_Countries')
                },
                'areas_of_interest': {
                    'pbi': item.get('PBI'),
                    'hpc': item.get('HPC'),
                    'preventing_or_relieving_suffering_of_animals': item.get('Preventing_or_relieving_suffering_of_animals'),
                    'aborigninal_or_tsi': item.get('Aboriginal_or_TSI'),
                    'adults': item.get('Adults'),
                    'advancing_health': item.get('Advancing_health'),
                    'advancing_education': item.get('Advancing_education'),
                    'advancing_religion': item.get('Advancing_religion'),
                    'advancing_culture': item.get('Advancing_culture'),
                    'advancing_social_or_public_welfare': item.get('Advancing_social_or_public_welfare'),
                    'advancing_natural_environment': item.get('Advancing_natual_environment'),
                    'advancing_security_or safety_of_australia_or_australian_public': item.get('Advancing_security_or_safety_of_Australia_or_Australian_public'),
                    'aged_persons': item.get('Aged_Persons'),
                    'childern': item.get('Children'),
                    'communities_overseas': item.get('Communities_Overseas'),
                    'early_childhood': item.get('Early_Childhood'),
                    'ethnic_groups': item.get('Ethnic_Groups'),
                    'families': item.get('Families'),
                    'females': item.get('Females'),
                    'financially_disadvantaged': item.get('Financially_Disadvantaged'),
                    'gay_lesbian_bisexual': item.get('Gay_Lesbian_Bisexual'),
                    'general_community_in_australia': item.get('General_Community_in_Australia'),
                    'males': item.get('Males'),
                    'migrants_refugees_or_asylum_seekers': item.get('Migrants_Refugees_or_Asylum_Seekers'),
                    'other_beneficiaries': item.get('Other_Beneficiaries'),
                    'other_charities': item.get('Other_Charities'),
                    'promote_or_oppose_a_change_to_law__government_poll_or_prac': item.get('Promote_or_oppose_a_change_to_law__government_poll_or_prac'),
                    'promoting_or_protecting_human_rights': item.get('Promoting_or_protecting_human_rights'),
                    'promoting_reconciliation__mutual_respect_and_tolerance': item.get('Promoting_reconciliation__mutual_respect_and_tolerance'),
                    'purposes_beneficial_to_ther_general_public_and_other_analogous': item.get('Purposes_beneficial_to_ther_general_public_and_other_analogous'),
                    'people_at_risk_of_homelessness': item.get('People_at_risk_of_homelessness'),
                    'people_with_chronic_illness': item.get('People_with_Chronic_Illness'),
                    'people_with_disabilities': item.get('People_with_Disabilities'),
                    'pre_post_releaseoffenders': item.get('Pre_Post_Release_Offenders'),
                    'rural_regional_remote_communities': item.get('Rural_Regional_Remote_Communities'),
                    'unemployed_person': item.get('Unemployed_Person'),
                    'veterans_or_their_families': item.get('Veterans_or_their_families'),
                    'victims_of_crime': item.get('Victims_of_crime'),
                    'victims_of_disasters': item.get('Victims_of_Disasters'),
                    'youth': item.get('Youth')
                },
                'updated_at': datetime.now().isoformat(),
                # Add more fields as needed
            })
        return transformed

    @staticmethod
    def transform_nsw_assoc_data(raw_data):
        transformed = []
        for item in raw_data:
            transformed.append({
                'organisation_number': item.get('organisation_number'),
                'name': item.get('name'),
                'organisation_type': item.get('organisation_type'),
                'status': item.get('status'),
                'date_registered': item.get('date_registered'),
                'date_removed': item.get('date_removed'),
                'registered_office_address': item.get('registered_office_address'),
                'organisation_id': item.get('organisation_id'),
                'updated_at': datetime.now().isoformat(),
                # Add more fields as needed
            })
        return transformed