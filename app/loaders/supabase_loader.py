from typing import Dict, List, Any, Union
from supabase import create_client, Client
from config.config import Config

class SupabaseLoader:
    def __init__(self):
        if not Config.SUPABASE_URL or not Config.SUPABASE_KEY:
            raise ValueError("Supabase URL and Key must be provided")
        
        self.supabase: Client = create_client(
            str(Config.SUPABASE_URL),
            str(Config.SUPABASE_KEY)
        )

    def upsert_organizations(self, data: List[Dict[str, Any]]) -> List[Any]:
        """
        Upsert data to organizations table.
        
        Args:
            data: List of organization records to upsert
            
        Returns:
            List containing the response data from Supabase
        """
        try:
            result = self.supabase.table('organizations').upsert(data).execute()
            return result.data
        except Exception as e:
            raise Exception(f"Error upserting to Supabase: {str(e)}")

    def upsert_charity_details(self, data: List[Dict[str, Any]]) -> List[Any]:
        """
        Upsert data to charity_details table.
        
        Args:
            data: List of charity detail records to upsert
            
        Returns:
            List containing the response data from Supabase
        """
        try:
            result = self.supabase.table('charity_details').upsert(data).execute()
            return result.data
        except Exception as e:
            raise Exception(f"Error upserting to Supabase: {str(e)}")