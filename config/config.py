import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SUPABASE_URL = os.getenv('PUBLIC_SUPABASE_URL')
    SUPABASE_KEY = os.getenv('PUBLIC_SUPABASE_ANON_KEY')
    REDIS_URL = os.getenv('REDIS_URL') or 'localhost:6379'
    REDIS_HOST = os.getenv('REDIS_HOST') or 'localhost'
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    REDIS_USERNAME = os.getenv('PRIVATE_REDIS_USERNAME') or ''
    REDIS_PASSWORD = os.getenv('PRIVATE_REDIS_PASSWORD') or ''
    ABN_GUID = os.getenv('PRIVATE_ABN_SEARCH_GUID')
    CACHE_EXPIRATION = 3600  # 1 hour