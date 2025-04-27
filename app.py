# app.py

import streamlit as st
import pandas as pd
from pymongo import MongoClient
from neo4j import GraphDatabase

# --- MongoDB Setup ---
from script_mongo import load_mongo_data 

# --- Neo4j Setup ---
NEO4J_URI = "bolt://localhost:7687"  # or your Neo4j server address
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "your_password"
