from kafka import KafkaProducer
import requests
import time
import json
from datetime import datetime, timedelta

# Configuration
COINMARKETCAP_API_KEY = "YOUR_COINMARKETCAP_API_KEY"  # Replace
CMC_BASE_URL = "https://pro-api.coinmarketcap.com/v1"
HEADERS = {'X-CMC_PRO_API_KEY': COINMARKETCAP_API_KEY}

# Supported cryptocurrencies with their CoinMarketCap IDs
COIN_IDS = {
    "bitcoin": 1,        # BTC
    "ethereum": 1027,    # ETH
    "ripple": 52,        # XRP
}

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    request_timeout_ms=30000
)

# Store rolling price and volume history for 1h
price_volume_history = {coin: [] for coin in COIN_IDS}
ONE_HOUR_SECONDS = 3600

def prune_history(coin, current_ts):
    """Remove data points older than 1 hour"""
    cutoff = current_ts - ONE_HOUR_SECONDS
    price_volume_history[coin] = [
        (ts, price, vol) for ts, price, vol in price_volume_history[coin] if ts >= cutoff
    ]

def safe_api_request():
    """Handle API requests with retries and rate limiting"""
    url = f"{CMC_BASE_URL}/cryptocurrency/quotes/latest"
    params = {'id': ','.join(str(cid) for cid in COIN_IDS.values()), 'convert': 'USD'}
    
    for attempt in range(3):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=10)
            response.raise_for_status()
            return response.json()['data']
        except requests.exceptions.RequestException as e:
            print(f"⚠️ API attempt {attempt+1}/3 failed: {e}")
            time.sleep(2)
    return None

def calculate_metrics(coin_name, current_price, current_volume, ts):
    """Calculate 1h and 24h metrics for a coin"""
    # Add current point to history
    price_volume_history[coin_name].append((ts, current_price, current_volume))
    prune_history(coin_name, ts)
    
    # Calculate 1h metrics
    prices_1h = [p for _, p, _ in price_volume_history[coin_name]]
    high_1h = max(prices_1h) if prices_1h else current_price
    low_1h = min(prices_1h) if prices_1h else current_price
    
    volumes_1h = [v for _, _, v in price_volume_history[coin_name]]
    if len(volumes_1h) >= 2:
        volume_1h = max(0, volumes_1h[-1] - volumes_1h[0])
    else:
        volume_1h = current_volume
        
    return {
        'high_1h': high_1h,
        'low_1h': low_1h,
        'volume_1h': volume_1h
    }

def main_loop():
    """Main producer loop"""
    while True:
        try:
            data = safe_api_request()
            if not data:
                print("🔄 No data, sleeping 60 seconds...")
                time.sleep(60)
                continue

            ts = int(time.time())
            output = {
                "timestamp": ts,
                "prices": {},
                "metrics": {},
                "alerts": [],
                "signals": {}
            }

            for coin_name, coin_id in COIN_IDS.items():
                coin_key = str(coin_id)
                if coin_key not in data:
                    continue

                quote = data[coin_key]['quote']['USD']
                current_price = quote['price']
                current_volume = quote.get('volume_24h', 0)
                
                # Calculate metrics
                metrics = calculate_metrics(coin_name, current_price, current_volume, ts)
                
                output['prices'][coin_name] = {
                    'price': current_price,
                    '24h_change': quote.get('percent_change_24h', 0),
                    'high_24h': quote.get('high_24h', current_price),
                    'low_24h': quote.get('low_24h', current_price),
                    '24h_volume': current_volume,
                    **metrics
                }

            producer.send('crypto-prices', output)
            print(f"✅ Sent data at {datetime.now().strftime('%H:%M:%S')}")

            time.sleep(300)  # 5 minutes interval

        except KeyboardInterrupt:
            print("🛑 Producer stopped.")
            break
        except Exception as e:
            print(f"🔥 Error: {type(e).__name__}: {str(e)[:200]}")
            time.sleep(30)

if __name__ == "__main__":
    main_loop()