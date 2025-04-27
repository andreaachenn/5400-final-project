import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import logging
from datetime import datetime

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_mongo_data():
    """Load documents from the 'complaints' collection."""
    client = MongoClient("mongodb://localhost:27017/")
    db = client["nyc_311_db"]
    collection = db["complaints"]

    documents = list(collection.find({}, {'_id': 0}))
    client.close()
    return documents

def setup_database():
    """Prepare and insert cleaned data into MongoDB."""
    logger.info("Reading cleaned data from CSV...")
    df = pd.read_csv("Cleaned_data.csv", nrows=100000)

    column_mapping = {
        "Created_date": "created_date",
        "Complaint_type": "complaint_type",
        "Incident_zip": "incident_zip",
        "Borough": "borough",
        "Descriptor": "descriptor",
        "Status": "status",
        "Closed_Date": "closed_date"
    }

    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)
        else:
            df[new_name] = "UNKNOWN"

    required_fields = list(column_mapping.values())
    for field in required_fields:
        if field not in df.columns:
            df[field] = "UNKNOWN"
        else:
            df[field] = df[field].fillna("UNKNOWN").astype(str).str.strip()

    for date_field in ["created_date", "closed_date"]:
        df[date_field] = pd.to_datetime(df[date_field], errors="coerce").dt.strftime("%Y-%m-%d")
        df[date_field] = df[date_field].replace("NaT", None)

    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["nyc_311_db"]
        collection = db["complaints"]
        collection.drop()
        logger.info("Dropped old collection.")

        records = df[required_fields].to_dict("records")
        collection.insert_many(records, ordered=False)
        logger.info(f"Inserted {collection.count_documents({})} records.")

        collection.create_index("incident_zip")
        collection.create_index("complaint_type")
        collection.create_index([("created_date", 1), ("incident_zip", 1)])
        logger.info("Indexes created.")
    except (ConnectionFailure, OperationFailure) as e:
        logger.error(f"Database operation failed: {e}")
        raise
    finally:
        client.close()
        logger.info("MongoDB connection closed.")
