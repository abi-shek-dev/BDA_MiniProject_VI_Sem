import streamlit as st
import pandas as pd
from pymongo import MongoClient
import time

# Connect to local MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_db"]
collection = db["sentiment_trends"]

st.set_page_config(page_title="Live Sentiment Dashboard", layout="wide")
st.title("🛒 Real-Time E-Commerce Sentiment Analyzer")

# Placeholder for auto-refreshing content
placeholder = st.empty()

# Loop to continuously fetch data and update the UI
while True:
    # Fetch all data from MongoDB (in a real app, you'd filter by recent timestamps)
    data = list(collection.find({}, {"_id": 0}))
    
    with placeholder.container():
        if data:
            df = pd.DataFrame(data)
            
            # Aggregate data: Average sentiment per product
            summary_df = df.groupby('product_name').agg(
                average_sentiment=('sentiment_score', 'mean'),
                review_count=('review_text', 'count')
            ).reset_index()

            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Average Sentiment by Product")
                # Bar chart showing sentiment
                st.bar_chart(data=summary_df, x='product_name', y='average_sentiment')
                
            with col2:
                st.subheader("Total Mentions")
                st.bar_chart(data=summary_df, x='product_name', y='review_count')
                
            st.write("### Raw Processed Data Feed")
            st.dataframe(df.tail(10)) # Show the 10 most recent rows
            
        else:
            st.info("Waiting for data stream... Start the Kafka Producer!")

    # Refresh the dashboard every 3 seconds
    time.sleep(3)