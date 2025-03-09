import json
import threading, time, datetime
from flask import Flask, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)
CORS(app)

engine = create_engine('sqlite:///orchestrator.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()

class OrchestratorLog(Base):
    __tablename__ = 'orchestrator_logs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String)
    order_id = Column(Integer)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(engine)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
ORCHESTRATOR_TOPIC = 'orchestrator_service'
ORDER_TOPIC = 'order_service'
PAYMENT_TOPIC = 'payment_service'
RESTAURANT_TOPIC = 'restaurant_service'
DELIVERY_TOPIC = 'delivery_service'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def publish_message(topic, message):
    producer.send(topic, message)
    producer.flush()

def log_event(event_type, order_id):
    session = Session()
    rec = OrchestratorLog(event_type=event_type, order_id=order_id)
    session.add(rec)
    session.commit()
    session.close()

def consume_messages():
    consumer = KafkaConsumer(
        ORCHESTRATOR_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='orchestrator_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for msg in consumer:
        message = msg.value
        msg_type = message.get("type")
        order_id = message.get("order_id")
        print(f"[Orchestrator] Received {msg_type} for order {order_id}")
        log_event(msg_type, order_id)
        mode = message.get("mode", "orchestration")
        fail_payment = message.get("fail_payment", False)
        retry_payment = message.get("retry_payment", 0)
        fail_restaurant = message.get("fail_restaurant", False)
        retry_restaurant = message.get("retry_restaurant", 0)
        fail_delivery = message.get("fail_delivery", False)
        retry_delivery = message.get("retry_delivery", 0)

        if msg_type == "OrderCreatedEvent":
            # Gửi PaymentReserveCommand
            cmd = {
                "type": "PaymentReserveCommand",
                "order_id": order_id,
                "mode": "orchestration",
                "fail_payment": fail_payment,
                "retry_payment": retry_payment,
                "fail_restaurant": fail_restaurant,
                "retry_restaurant": retry_restaurant,
                "fail_delivery": fail_delivery,
                "retry_delivery": retry_delivery
            }
            publish_message(PAYMENT_TOPIC, cmd)
            print(f"[Orchestrator] -> PaymentReserveCommand for order {order_id}")

        elif msg_type == "PaymentReservedEvent":
            cmd = {
                "type": "RestaurantConfirmCommand",
                "order_id": order_id,
                "mode": "orchestration",
                "fail_restaurant": fail_restaurant,
                "retry_restaurant": retry_restaurant,
                "fail_delivery": fail_delivery,
                "retry_delivery": retry_delivery
            }
            publish_message(RESTAURANT_TOPIC, cmd)
            print(f"[Orchestrator] -> RestaurantConfirmCommand for order {order_id}")

        elif msg_type == "PaymentFailedEvent":
            event = {"type": "PaymentFailedEvent", "order_id": order_id, "mode": "orchestration"}
            publish_message(ORDER_TOPIC, event)
            print(f"[Orchestrator] -> PaymentFailedEvent for order {order_id}")

        elif msg_type == "RestaurantConfirmedEvent":
            cmd = {
                "type": "DeliveryScheduleCommand",
                "order_id": order_id,
                "mode": "orchestration",
                "fail_delivery": fail_delivery,
                "retry_delivery": retry_delivery
            }
            publish_message(DELIVERY_TOPIC, cmd)
            print(f"[Orchestrator] -> DeliveryScheduleCommand for order {order_id}")

        elif msg_type == "RestaurantFailedEvent":
            cmd = {"type": "RefundPaymentCommand", "order_id": order_id, "mode": "orchestration"}
            publish_message(PAYMENT_TOPIC, cmd)
            publish_message(ORDER_TOPIC, {"type": "RestaurantFailedEvent", "order_id": order_id, "mode": "orchestration"})
            print(f"[Orchestrator] -> RefundPaymentCommand & RestaurantFailedEvent for order {order_id}")

        elif msg_type == "DeliveryScheduledEvent":
            publish_message(ORDER_TOPIC, {"type": "DeliveryScheduledEvent", "order_id": order_id, "mode": "orchestration"})
            print(f"[Orchestrator] -> DeliveryScheduledEvent for order {order_id} (completed)")

        elif msg_type == "DeliveryFailedEvent":
            cmd1 = {"type": "CancelRestaurantCommand", "order_id": order_id, "mode": "orchestration"}
            cmd2 = {"type": "RefundPaymentCommand", "order_id": order_id, "mode": "orchestration"}
            publish_message(RESTAURANT_TOPIC, cmd1)
            publish_message(PAYMENT_TOPIC, cmd2)
            publish_message(ORDER_TOPIC, {"type": "DeliveryFailedEvent", "order_id": order_id, "mode": "orchestration"})
            print(f"[Orchestrator] -> CancelRestaurant & RefundPayment for order {order_id}")

threading.Thread(target=consume_messages, daemon=True).start()

@app.route("/orchestrator_logs", methods=["GET"])
def get_orchestrator_logs():
    session = Session()
    logs = session.query(OrchestratorLog).order_by(OrchestratorLog.id.asc()).all()
    data = []
    for l in logs:
        data.append({
            "id": l.id,
            "event_type": l.event_type,
            "order_id": l.order_id,
            "created_at": l.created_at.isoformat()
        })
    session.close()
    return jsonify(data)

if __name__ == "__main__":
    app.run(port=7005, debug=True)
