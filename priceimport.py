import requests
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extras import Json
from datetime import datetime
import time

# === Supabase DB connection settings ===
import os
import psycopg2

conn = psycopg2.connect(
    dbname=os.getenv("postgres"),
    user=os.getenv("postgres"),
    password=os.getenv("Awooga133100!!!"),
    host=os.getenv("db.ksbpsntlkmgsenxjeqpg.supabase.co"),
    port=os.getenv("5432")
)
cur = conn.cursor()

print("📡 Updating card_prices from Scryfall...")

url = "https://api.scryfall.com/cards/search?q=%2A&unique=prints"
insert_count = 0

while url:
    print(f"🔄 Fetching: {url}")
    response = requests.get(url)
    data = response.json()

    if 'data' not in data:
        print("❌ Error: Unexpected response from Scryfall")
        print(data)
        break

    price_rows = []

    for card in data['data']:
        name = card.get('name')
        set_code = card.get('set')
        collector_number = card.get('collector_number')
        prices = card.get('prices', {})

        for price_type in ['usd', 'usd_foil', 'usd_etched']:
            price_value = prices.get(price_type)
            if price_value:
                price_rows.append((
                    name,
                    set_code,
                    collector_number,
                    price_type,
                    float(price_value),
                    datetime.utcnow()
                ))

    # Bulk insert into card_prices
    if price_rows:
        execute_values(cur, """
            INSERT INTO card_prices (
                card_name, set_code, collector_number,
                price_type, price, recorded_at
            ) VALUES %s
        """, price_rows)
        conn.commit()

    insert_count += len(price_rows)
    url = data.get('next_page') if data.get('has_more') else None
    time.sleep(0.1)  # light delay to avoid API throttling

print(f"✅ Done. Inserted {insert_count:,} new price records.")
cur.close()
conn.close()