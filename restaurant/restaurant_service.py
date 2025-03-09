# restaurant_service.py
import json
import threading, time
import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)
CORS(app)
engine = create_engine('sqlite:///restaurant.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()

class RestaurantOrder(Base):
    __tablename__ = 'restaurant_orders'
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(engine)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
RESTAURANT_TOPIC = 'restaurant_service'
ORDER_TOPIC = 'order_service'
ORCHESTRATOR_TOPIC = 'orchestrator_service'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def publish_message(topic, message):
    producer.send(topic, message)
    producer.flush()

def consume_messages():
    consumer = KafkaConsumer(
        RESTAURANT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='restaurant_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    session = Session()
    for msg in consumer:
        message = msg.value
        msg_type = message.get("type")
        order_id = message.get("order_id")
        print(f"[Restaurant] Received {msg_type} for order {order_id}")

        fail_restaurant = message.get("fail_restaurant", False)
        retry_restaurant = message.get("retry_restaurant", 0)
        mode = message.get("mode", "choreography")

        if msg_type == "RestaurantConfirmCommand":
            ro = RestaurantOrder(order_id=order_id, status="")
            if fail_restaurant:
                if retry_restaurant > 0:
                    new_retry = retry_restaurant - 1
                    time.sleep(1)
                    cmd = {
                        "type": "RestaurantConfirmCommand",
                        "order_id": order_id,
                        "mode": mode,
                        "fail_restaurant": True,
                        "retry_restaurant": new_retry,
                        "fail_delivery": message.get("fail_delivery", False),
                        "retry_delivery": message.get("retry_delivery", 0)
                    }
                    publish_message(RESTAURANT_TOPIC, cmd)
                    print(f"[Restaurant] Retry RestaurantConfirmCommand (order {order_id}), remain: {new_retry}")
                else:
                    ro.status = "failed"
                    session.add(ro)
                    session.commit()
                    event = {"type": "RestaurantFailedEvent", "order_id": order_id, "mode": mode}
                    if mode == "choreography":
                        publish_message(ORDER_TOPIC, event)
                    else:
                        publish_message(ORCHESTRATOR_TOPIC, event)
                    print(f"[Restaurant] RestaurantFailedEvent => {mode} (order {order_id})")
            else:
                ro.status = "confirmed"
                session.add(ro)
                session.commit()
                event = {
                    "type": "RestaurantConfirmedEvent",
                    "order_id": order_id,
                    "mode": mode,
                    "fail_delivery": message.get("fail_delivery", False),
                    "retry_delivery": message.get("retry_delivery", 0)
                }
                if mode == "choreography":
                    publish_message(ORDER_TOPIC, event)
                else:
                    publish_message(ORCHESTRATOR_TOPIC, event)
                print(f"[Restaurant] RestaurantConfirmedEvent => {mode} (order {order_id})")

        elif msg_type == "CancelRestaurantCommand":
            ro = RestaurantOrder(order_id=order_id, status="cancelled")
            session.add(ro)
            session.commit()
            print(f"[Restaurant] Cancelled order {order_id}")

    session.close()

def start_consumer():
    thread = threading.Thread(target=consume_messages, daemon=True)
    thread.start()

@app.route("/restaurant_orders", methods=["GET"])
def get_restaurant_orders():
    session = Session()
    ros = session.query(RestaurantOrder).all()
    result = []
    for r in ros:
        result.append({
            "id": r.id,
            "order_id": r.order_id,
            "status": r.status,
            "created_at": r.created_at.isoformat()
        })
    session.close()
    return jsonify(result)

if __name__ == "__main__":
    start_consumer()
    app.run(port=6002, debug=True)
