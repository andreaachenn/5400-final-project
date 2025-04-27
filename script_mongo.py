import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import logging
from datetime import datetime

def load_mongo_data():
    client = MongoClient("mongodb://localhost:27017/")  # or your MongoDB URI
    db = client["your_database_name"]
    collection = db["your_collection_name"]
    
    # Fetch documents
    documents = list(collection.find({}, {'_id': 0}))  # Exclude the MongoDB _id field
    return documents
    
# Set up logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Read the cleaned data (100,000 rows for demo, as per  code)
logger.info("Reading cleaned data from Cleaned_data.csv...")
df = pd.read_csv("Cleaned_data.csv", nrows=100000)

# Map DataFrame column names to expected MongoDB field names (lowercase)
column_mapping = {
    "Created_date": "created_date",
    "Complaint_type": "complaint_type",
    "Incident_zip": "incident_zip",
    "Borough": "borough",
    "Descriptor": "descriptor",
    "Status": "status",
    "Closed_Date": "closed_date"
}

# Rename columns to match MongoDB schema
for old_name, new_name in column_mapping.items():
    if old_name in df.columns:
        df.rename(columns={old_name: new_name}, inplace=True)
    else:
        logger.warning(f"Field {old_name} not found in DataFrame. Adding with default 'UNKNOWN'.")
        df[new_name] = "UNKNOWN"

# Ensure all required fields are present and standardized
required_fields = ["created_date", "complaint_type", "incident_zip", "borough", "descriptor", "status", "closed_date"]
for field in required_fields:
    if field not in df.columns:
        logger.warning(f"Field {field} not found in DataFrame. Adding with default 'UNKNOWN'.")
        df[field] = "UNKNOWN"
    else:
        # Fill missing values
        df[field] = df[field].fillna("UNKNOWN")
        # Convert to string and strip whitespace for consistency
        df[field] = df[field].astype(str).str.strip()

# Ensure date fields are in the correct format (YYYY-MM-DD) or None for MongoDB
for date_field in ["created_date", "closed_date"]:
    df[date_field] = pd.to_datetime(df[date_field], errors="coerce").dt.strftime("%Y-%m-%d")
    df[date_field] = df[date_field].replace("NaT", None)  # MongoDB prefers None for missing dates

# Log the first few rows for verification
logger.info("First few rows of cleaned DataFrame:")
logger.info(df[required_fields].head().to_string())

# Connect to MongoDB without authentication (since no admin user role exists)
try:
    client = MongoClient("mongodb://localhost:27017/")
    logger.info("Connected to MongoDB successfully.")
except ConnectionFailure as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    raise

# Create or access the database and collection
db = client["nyc_311_db"]
collection = db["complaints"]

# Drop existing collection to avoid duplicates (optional, for fresh start)
collection.drop()
logger.info("Dropped existing complaints collection for fresh import.")

# Insert the cleaned data into MongoDB with error handling
try:
    records = df[required_fields].to_dict("records")
    collection.insert_many(records, ordered=False)  # ordered=False for better performance
    logger.info(f"Inserted {collection.count_documents({})} records into MongoDB.")
except OperationFailure as e:
    logger.error(f"Failed to insert records into MongoDB: {e}")
    raise

# Create indexes for efficient querying
try:
    collection.create_index("incident_zip")
    logger.info("Created index on incident_zip for efficient querying.")
    collection.create_index("complaint_type")
    logger.info("Created index on complaint_type for efficient querying.")
    collection.create_index([("created_date", 1), ("incident_zip", 1)])
    logger.info("Created compound index on created_date and incident_zip for time-based ZIP queries.")
except OperationFailure as e:
    logger.error(f"Failed to create indexes: {e}")
    raise

# Skip access control since MongoDB is not configured with authentication
logger.info("Skipping access control setup since MongoDB authentication is not enabled.")

# Validate the stored data by querying a sample
logger.info("Validating stored data...")
sample_docs = collection.find({"incident_zip": "10001"}).limit(5)
sample_count = 0
logger.info("Sample records for ZIP code 10001:")
for doc in sample_docs:
    logger.info(doc)
    sample_count += 1
if sample_count == 0:
    logger.warning("No records found for ZIP code 10001. Check data integrity.")

# Close the MongoDB connection
client.close()
logger.info("MongoDB connection closed.")
