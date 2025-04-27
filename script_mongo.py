import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_mongo_data():
    """Connects to MongoDB and loads documents from the 'complaints' collection."""
    client = MongoClient("mongodb://localhost:27017/")
    db = client["nyc_311_db"]
    collection = db["complaints"]

    # Fetch documents
    documents = list(collection.find({}, {'_id': 0}))  # Exclude the MongoDB _id field
    client.close()
    return documents

def main():
    # Read the cleaned data
    logger.info("Reading cleaned data from Cleaned_data.csv...")
    df = pd.read_csv("Cleaned_data.csv", nrows=100000)

    # Map DataFrame columns to expected MongoDB field names
    column_mapping = {
        "Created_date": "created_date",
        "Complaint_type": "complaint_type",
        "Incident_zip": "incident_zip",
        "Borough": "borough",
        "Descriptor": "descriptor",
        "Status": "status",
        "Closed_Date": "closed_date"
    }

    # Rename columns
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)
        else:
            logger.warning(f"Field {old_name} not found. Adding '{new_name}' with default 'UNKNOWN'.")
            df[new_name] = "UNKNOWN"

    # Ensure all required fields exist
    required_fields = list(column_mapping.values())
    for field in required_fields:
        if field not in df.columns:
            logger.warning(f"Field {field} missing in DataFrame. Filling with 'UNKNOWN'.")
            df[field] = "UNKNOWN"
        else:
            df[field] = df[field].fillna("UNKNOWN").astype(str).str.strip()

    # Format date fields
    for date_field in ["created_date", "closed_date"]:
        df[date_field] = pd.to_datetime(df[date_field], errors="coerce").dt.strftime("%Y-%m-%d")
        df[date_field] = df[date_field].replace("NaT", None)

    # Log a sample
    logger.info("Sample of cleaned data:")
    logger.info(df[required_fields].head().to_string())

    # Connect to MongoDB
    try:
        client = MongoClient("mongodb://localhost:27017/")
        logger.info("Connected to MongoDB.")
    except ConnectionFailure as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise

    # Insert data
    db = client["nyc_311_db"]
    collection = db["complaints"]

    # Drop and recreate collection
    collection.drop()
    logger.info("Dropped existing collection.")

    try:
        records = df[required_fields].to_dict("records")
        collection.insert_many(records, ordered=False)
        logger.info(f"Inserted {collection.count_documents({})} records into MongoDB.")
    except OperationFailure as e:
        logger.error(f"Insert failed: {e}")
        raise

    # Create indexes
    try:
        collection.create_index("incident_zip")
        collection.create_index("complaint_type")
        collection.create_index([("created_date", 1), ("incident_zip", 1)])
        logger.info("Indexes created.")
    except OperationFailure as e:
        logger.error(f"Index creation failed: {e}")
        raise

    # Validation
    logger.info("Validating inserted data...")
    sample_docs = collection.find({"incident_zip": "10001"}).limit(5)
    for doc in sample_docs:
        logger.info(doc)

    client.close()
    logger.info("MongoDB connection closed.")

if __name__ == "__main__":
    main()
