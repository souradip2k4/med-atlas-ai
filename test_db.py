from IDP.storage.database import DatabaseManager

db = DatabaseManager()
try:
    df = db.read_delta("facility_records").select("facility_id").limit(10)
    print("RECORDS", df.collect())
except Exception as e:
    print(e)
