# app.py

import streamlit as st
import pandas as pd
from pymongo import MongoClient
from neo4j import GraphDatabase

# --- MongoDB Setup ---
from script_mongo import load_mongo_data 

# --- Neo4j Setup ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Andrea0411"
def get_neo4j_data():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run("MATCH (n) RETURN n LIMIT 10")  # Simple query
        records = []
        for record in result:
            records.append(record["n"])
    driver.close()
    return records
  # --- Streamlit UI ---
st.title("Big Data Final Project Dashboard")

# Sidebar
st.sidebar.header("Navigation")
page = st.sidebar.selectbox("Choose a page", ["Home", "MongoDB Data", "Neo4j Data"])

# Load MongoDB data
try:
    mongo_data = load_mongo_data()
except Exception as e:
    mongo_data = []
    st.sidebar.error(f"MongoDB connection failed: {e}")

# Page Logic
if page == "Home":
    st.write("Enhancing Real Estate Decision-Making 🚀")
    st.write("This app connects to MongoDB and Neo4j to explore data.")
    
elif page == "MongoDB Data":
    st.header("MongoDB: Sample Documents")
    if mongo_data:
        st.write(pd.DataFrame(mongo_data))
    else:
        st.warning("No data found or MongoDB connection error.")
        
elif page == "Neo4j Data":
    st.header("Neo4j: Sample Nodes")
    try:
        neo4j_data = get_neo4j_data()
        for node in neo4j_data:
            st.json(dict(node))
    except Exception as e:
        st.error(f"Neo4j connection failed: {e}")
