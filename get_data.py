import pandas as pd
import os

# Create data folder if it doesn't exist
os.makedirs('data', exist_ok=True)

print("Downloading dataset from public repositories...")

# We use a list of backup URLs just in case one goes down!
urls = [
    "https://raw.githubusercontent.com/NadimKawwa/WomeneCommerce/master/Womens%20Clothing%20E-Commerce%20Reviews.csv",
    "https://raw.githubusercontent.com/AFAgarap/ecommerce-reviews-analysis/master/Womens%20Clothing%20E-Commerce%20Reviews.csv"
]

df = None
for url in urls:
    try:
        print(f"Trying to fetch data from GitHub...")
        df = pd.read_csv(url)
        print("✅ Data downloaded successfully!")
        break # Exit the loop if successful
    except Exception as e:
        print("⚠️ Link failed, trying the backup link...")

if df is not None:
    print("Cleaning and formatting data...")
    # Keep only the columns we need and drop empty reviews
    df = df[['Clothing ID', 'Class Name', 'Review Text']].dropna()

    # Rename columns to perfectly match our Spark schema!
    df.columns = ['product_id', 'product_name', 'review_text']

    # Save to our data folder
    df.to_csv('data/actual_reviews.csv', index=False)
    print(f"🎉 SUCCESS! Saved {len(df)} real reviews to data/actual_reviews.csv!")
else:
    print("❌ Failed to download from all sources. Check your internet connection.")