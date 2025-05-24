from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime, timedelta
import requests
import time

# MongoDB setup
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["crypto_db"]
mongo_collection = mongo_db["prices"]

# Supported cryptocurrencies
COINS_LIST = [
    'bitcoin',
    'ethereum',
    'ripple',
]

# Clear old data on restart
mongo_collection.delete_many({})
print("⚠️ Cleared all documents in 'prices' collection.")

def backfill_historical(coins, hours=48):
    """Backfill historical data from CoinGecko"""
    print(f"🔄 Backfilling last {hours} hours for coins: {coins}")
    merged_data = {}
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)

    for coin in coins:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range"
        params = {
            'vs_currency': 'usd',
            'from': int(start_time.timestamp()),
            'to': int(end_time.timestamp())
        }

        while True:
            response = requests.get(url, params=params)
            if response.status_code != 429:
                break
            print("⚠️ Rate limited. Sleeping 10s...")
            time.sleep(10)

        response.raise_for_status()
        data = response.json()

        for ts_ms, price in data['prices']:
            ts = int(ts_ms // 1000)
            current_time = datetime.fromtimestamp(ts)
            if ts not in merged_data:
                merged_data[ts] = {"prices": {}, "timestamp": current_time}
            merged_data[ts]["prices"][coin] = {"usd": price}

        time.sleep(2)  # Be nice to the API

    # Filter for 5-min intervals
    sorted_ts = sorted(merged_data.keys())
    final_data = {}
    last_ts = None
    for ts in sorted_ts:
        if last_ts is None or ts - last_ts >= 300:
            final_data[ts] = merged_data[ts]
            last_ts = ts

    # Insert into MongoDB
    for ts in sorted(final_data.keys()):
        try:
            mongo_collection.insert_one(final_data[ts])
            print(f"✅ Inserted backfill data for {final_data[ts]['timestamp']}")
        except Exception as e:
            print(f"❌ Failed to insert backfill for {final_data[ts]['timestamp']}: {e}")

    print(f"✅ Backfilled {len(final_data)} snapshots.")
    return final_data

def normalize_price_data(price_data):
    """Standardize price data structure"""
    if 'usd' not in price_data and 'price' in price_data:
        price_data['usd'] = price_data.pop('price')
    price_data.setdefault('24h_volume', 0)
    price_data.setdefault('24h_change', 0)
    price_data.setdefault('high_24h', price_data.get('usd', 0))
    price_data.setdefault('low_24h', price_data.get('usd', 0))
    price_data.setdefault('high_1h', price_data.get('usd', 0))
    price_data.setdefault('low_1h', price_data.get('usd', 0))
    price_data.setdefault('volume_1h', 0)
    return price_data

def is_valid_price_data(price_data):
    """Validate required fields"""
    required_keys = ['usd', '24h_change', 'high_24h', 'low_24h', 'high_1h', 'low_1h', 'volume_1h']
    return all(k in price_data for k in required_keys)

def start_consumer():
    """Main consumer loop"""
    consumer = KafkaConsumer(
        'crypto-prices',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='crypto-consumer-group'
    )

    print("=== Crypto Data Consumer Started ===")
    for message in consumer:
        data = message.value
        try:
            ts = data.get('timestamp')
            if ts is None:
                print("⚠️ Message without timestamp, skipping.")
                continue

            time_str = datetime.fromtimestamp(ts).strftime('%H:%M:%S')
            print(f"\n📊 Data @ {time_str}")

            for coin, price_data in data.get('prices', {}).items():
                price_data = normalize_price_data(price_data)

                if not is_valid_price_data(price_data):
                    print(f"⚠️ Skipping {coin} due to missing fields: {price_data}")
                    continue

                print(
                    f"  {coin.upper():<8} Price: ${price_data['usd']:<12,.2f} "
                    f"Change(24h): {price_data['24h_change']:.2f}% "
                    f"High(24h): ${price_data['high_24h']:<12,.2f} "
                    f"Low(24h): ${price_data['low_24h']:<12,.2f} "
                    f"High(1h): ${price_data['high_1h']:<12,.2f} "
                    f"Low(1h): ${price_data['low_1h']:<12,.2f} "
                    f"Volume(24h): {price_data['24h_volume']:,.2f} "
                    f"Volume(1h): {price_data['volume_1h']:,.2f}"
                )

            # Prepare MongoDB document
            mongo_doc = {
                "timestamp": datetime.fromtimestamp(ts),
                "prices": data.get('prices', {}),
                "metrics": data.get('metrics', {}),
                "metadata": {
                    "data_source": "CoinMarketCap",
                    "processing_time": datetime.now()
                }
            }

            mongo_collection.insert_one(mongo_doc)

        except Exception as e:
            print(f"❌ Error processing message: {e}")

if __name__ == "__main__":
    backfill_historical(COINS_LIST, hours=24)
    start_consumer()