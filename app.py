from flask import Flask
from supabase import create_client
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

supabase = create_client(os.getenv("PUBLIC_SUPABASE_URL") or "", os.getenv("PUBLIC_SUPABASE_ANON_KEY") or "")

@app.route("/upload")
def upload_data():
    df = pd.read_csv("metadata.csv")
    for row in df.to_dict(orient="records"):
        supabase.table("associations").insert(row).execute()
    return "Upload complete"

if __name__ == "__main__":
    app.run(debug=True)