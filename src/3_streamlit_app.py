import streamlit as st
import pandas as pd
from pymongo import MongoClient
import time
import plotly.express as px

# 1. Page Config MUST be the first command
st.set_page_config(page_title="Live Sentiment Dashboard", page_icon="📈", layout="wide")

# 2. Cache the database connection so we don't crash MongoDB
@st.cache_resource
def init_connection():
    client = MongoClient("mongodb://localhost:27017/")
    return client["ecommerce_db"]["sentiment_trends"]

collection = init_connection()

# Header Section
st.title("📈 Real-Time E-Commerce Sentiment Intelligence")
st.markdown("Live streaming data from **Kafka**, processed by **PySpark**, stored in **MongoDB**.")
st.divider()

# Placeholder for the live-updating content
placeholder = st.empty()

while True:
    # Fetch all data from MongoDB
    data = list(collection.find({}, {"_id": 0}))
    
    with placeholder.container():
        if data:
            df = pd.DataFrame(data)
            
            # --- ROW 1: TOP LEVEL KPI METRICS ---
            total_reviews = len(df)
            avg_sentiment = df['sentiment_score'].mean()
            positive_reviews = len(df[df['sentiment_score'] > 0])
            negative_reviews = len(df[df['sentiment_score'] < 0])
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Reviews Processed", f"{total_reviews:,}")
            col2.metric("Overall Average Sentiment", f"{avg_sentiment:.2f}")
            col3.metric("Positive Reviews 🟢", f"{positive_reviews:,}")
            col4.metric("Negative Reviews 🔴", f"{negative_reviews:,}")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # --- ROW 2: INTERACTIVE PLOTLY CHARTS ---
            # Group data for the charts
            summary_df = df.groupby('product_name').agg(
                average_sentiment=('sentiment_score', 'mean'),
                review_count=('review_text', 'count')
            ).reset_index()
            
            # Sort by most reviewed products to keep the chart clean
            summary_df = summary_df.sort_values(by='review_count', ascending=False).head(15)
            
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                st.subheader("Top Products by Volume & Sentiment")
                # Horizontal Bar chart colored by sentiment score
                fig1 = px.bar(
                    summary_df, 
                    x='review_count', 
                    y='product_name', 
                    color='average_sentiment',
                    orientation='h',
                    color_continuous_scale=['#EF553B', 'gray', '#00CC96'], # Red to Green
                    labels={'review_count': 'Total Mentions', 'product_name': '', 'average_sentiment': 'Sentiment'}
                )
                fig1.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=400)
                
                # FIXED: Added unique key using time.time() to prevent the Duplicate ID error!
                st.plotly_chart(fig1, use_container_width=True, key=f"bar_{time.time()}")
                
            with chart_col2:
                st.subheader("Overall Sentiment Distribution")
                # Create labels for a Donut chart
                df['Category'] = pd.cut(df['sentiment_score'], bins=[-1.1, -0.1, 0.1, 1.1], labels=['Negative', 'Neutral', 'Positive'])
                sentiment_counts = df['Category'].value_counts().reset_index()
                sentiment_counts.columns = ['Sentiment', 'Count']
                
                fig2 = px.pie(
                    sentiment_counts, 
                    names='Sentiment', 
                    values='Count',
                    color='Sentiment',
                    color_discrete_map={'Positive':'#00CC96', 'Neutral':'#636EFA', 'Negative':'#EF553B'},
                    hole=0.4 # Makes it a donut
                )
                fig2.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=400)
                
                # FIXED: Added unique key using time.time() to prevent the Duplicate ID error!
                st.plotly_chart(fig2, use_container_width=True, key=f"pie_{time.time()}")
                
            # --- ROW 3: ENHANCED LIVE DATA FEED ---
            st.divider()
            st.subheader("📝 Live Review Feed (Newest First)")
            
            # Prep table data: newest 10 rows on top
            display_df = df.tail(10).copy()
            display_df = display_df[['product_name', 'review_text', 'sentiment_score']]
            display_df = display_df.iloc[::-1] # Reverse order so newest is at the top
            
            # Color-code the sentiment column
            def color_sentiment(val):
                color = '#00CC96' if val > 0 else '#EF553B' if val < 0 else 'gray'
                return f'color: {color}; font-weight: bold;'
                
            styled_df = display_df.style.map(color_sentiment, subset=['sentiment_score'])
            
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
        else:
            st.info("⏳ Waiting for Kafka data stream... Start the producer!")
            
    # Refresh every 2 seconds
    time.sleep(2)