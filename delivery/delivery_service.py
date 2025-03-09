# delivery_service.py
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
engine = create_engine('sqlite:///delivery.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Delivery(Base):
    __tablename__ = 'deliveries'
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(engine)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
DELIVERY_TOPIC = 'delivery_service'
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
        DELIVERY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='delivery_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    session = Session()
    for msg in consumer:
        message = msg.value
        msg_type = message.get("type")
        order_id = message.get("order_id")
        print(f"[Delivery] Received {msg_type} for order {order_id}")

        fail_delivery = message.get("fail_delivery", False)
        retry_delivery = message.get("retry_delivery", 0)
        mode = message.get("mode", "choreography")

        if msg_type == "DeliveryScheduleCommand":
            d = Delivery(order_id=order_id, status="")
            if fail_delivery:
                if retry_delivery > 0:
                    new_retry = retry_delivery - 1
                    time.sleep(1)
                    cmd = {
                        "type": "DeliveryScheduleCommand",
                        "order_id": order_id,
                        "mode": mode,
                        "fail_delivery": True,
                        "retry_delivery": new_retry
                    }
                    publish_message(DELIVERY_TOPIC, cmd)
                    print(f"[Delivery] Retry DeliveryScheduleCommand (order {order_id}), remain: {new_retry}")
                else:
                    d.status = "failed"
                    session.add(d)
                    session.commit()
                    event = {"type": "DeliveryFailedEvent", "order_id": order_id, "mode": mode}
                    if mode == "choreography":
                        publish_message(ORDER_TOPIC, event)
                    else:
                        publish_message(ORCHESTRATOR_TOPIC, event)
                    print(f"[Delivery] DeliveryFailedEvent => {mode} (order {order_id})")
            else:
                d.status = "scheduled"
                session.add(d)
                session.commit()
                event = {"type": "DeliveryScheduledEvent", "order_id": order_id, "mode": mode}
                if mode == "choreography":
                    publish_message(ORDER_TOPIC, event)
                else:
                    publish_message(ORCHESTRATOR_TOPIC, event)
                print(f"[Delivery] DeliveryScheduledEvent => {mode} (order {order_id})")

    session.close()

def start_consumer():
    thread = threading.Thread(target=consume_messages, daemon=True)
    thread.start()

@app.route("/deliveries", methods=["GET"])
def get_deliveries():
    session = Session()
    dels = session.query(Delivery).all()
    result = []
    for d in dels:
        result.append({
            "id": d.id,
            "order_id": d.order_id,
            "status": d.status,
            "created_at": d.created_at.isoformat()
        })
    session.close()
    return jsonify(result)

if __name__ == "__main__":
    start_consumer()
    app.run(port=6003, debug=True)
