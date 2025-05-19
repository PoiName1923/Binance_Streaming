import requests
import websocket
import json
from binance.client import Client

from kafka import KafkaProducer
from kafka.errors import KafkaError

import logging
# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Hàm lấy về top 100 ticker có giá trị cao nhất trong ngày
def get_top_100() -> list:
    try:
        client = Client()
        # Tìm tất cả tickers
        all_tickers = client.get_all_tickers()
        # Lọc ra các cặp ticker USDT
        all_tickers_usdt = [ticker for ticker in all_tickers if 'USDT' in ticker['symbol']]
        # Chọn top 100 theo giá
        top_100_usdt_ticker = sorted(all_tickers_usdt, key=lambda x: float(x['price']), reverse=True)[:100]
        return [ticker['symbol'].lower() for ticker in top_100_usdt_ticker]
    except Exception as e:
        logger.error(f"Error when getting top 100 tickers binance : {e}")
        return []
# Tạo Kafka Product
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'binance_kline_data'

def init_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info("Kafka producer initialized successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        raise

# Hàm xử lý khi kết nối WebSocket bị lỗi
def on_error(ws, error):
    print("Error:", error)

# Hàm xử lý khi WebSocket đóng kết nối
def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

# Hàm xử lý khi kết nối WebSocket thành công
def on_open(ws):
    print("WebSocket opened")
    # Lấy top 100
    top_100 = get_top_100()
    # Check
    if not top_100:
        logger.error("No tickers available to subscribe")
        return
    # Tạo danh sách các streams kline 1 phút cho các tickers
    streams = [f"{symbol}@kline_1m" for symbol in top_100]
    # Giới hạn số lượng streams mỗi lần kết nối (Binance có giới hạn)
    batch_size = 50
    for i in range(0, len(streams), batch_size):
        batch = streams[i:i + batch_size]
        
        # Gửi yêu cầu đăng ký stream
        ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": batch,
            "id": i // batch_size + 1  # ID tăng dần cho mỗi batch
        }))
        logger.info(f"Subscribed to batch {i // batch_size + 1} with {len(batch)} streams")

# Hàm xử lý khi nhận dữ liệu từ WebSocket
def on_message(ws, message):
    data = json.loads(message)
    try:
    # Kiểm tra nếu là dữ liệu kline
        if 'k' in data:
            # Gửi dữ liệu đến Kafka
            ws.kafka_producer.send(KAFKA_TOPIC, value=data)
            logger.info(f"Sent data to Kafka to {data['s']} | Time {data['E']} ")
    except Exception as e:
         logger.error(f"Error processing message: {e}")

def start_websocket():
    # Khởi tạo Kafka producer
    try:
        kafka_producer = init_kafka_producer()
    except Exception as e:
        logger.error(f"Failed to start WebSocket due to Kafka initialization error: {e}")
        return None
    ws = websocket.WebSocketApp(
        "wss://fstream.binance.com/ws",
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close
    )
    ws.kafka_producer = kafka_producer
    return ws

if __name__ == "__main__":
    logger.info("Starting Binance WebSocket to Kafka pipeline")
    ws = start_websocket()
    
    if ws:
        try:
            ws.run_forever()
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            ws.kafka_producer.close()
            ws.close()
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            ws.kafka_producer.close()
            ws.close()