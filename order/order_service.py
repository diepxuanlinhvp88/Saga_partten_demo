# order_service.py
import json
import threading, datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)
CORS(app)
engine = create_engine('sqlite:///order.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer, primary_key=True)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(engine)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
ORDER_TOPIC = 'order_service'
ORCHESTRATOR_TOPIC = 'orchestrator_service'
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

def consume_messages():
    consumer = KafkaConsumer(
        ORDER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='order_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    session = Session()
    for msg in consumer:
        message = msg.value
        msg_type = message.get("type")
        order_id = message.get("order_id")
        print(f"[Order Service] Received {msg_type} for order {order_id}")

        order = session.query(Order).filter(Order.id == order_id).first()
        if not order:
            continue

        if msg_type == "PaymentReservedEvent":
            order.status = "payment_reserved"
            session.commit()
            # CHOREOGRAPHY: (mode=choreography) => RestaurantConfirmCommand
            # ORCHESTRATION: event này do Orchestrator handle => N/A
            if message.get("mode") == "choreography":
                cmd = {
                    "type": "RestaurantConfirmCommand",
                    "order_id": order_id,
                    "mode": "choreography",
                    "fail_restaurant": message.get("fail_restaurant", False),
                    "retry_restaurant": message.get("retry_restaurant", 0),
                    "fail_delivery": message.get("fail_delivery", False),
                    "retry_delivery": message.get("retry_delivery", 0)
                }
                publish_message(RESTAURANT_TOPIC, cmd)
                print(f"[Order Service] -> RestaurantConfirmCommand (choreography)")

        elif msg_type == "PaymentFailedEvent":
            order.status = "failed"
            session.commit()

        elif msg_type == "RestaurantConfirmedEvent":
            order.status = "restaurant_confirmed"
            session.commit()
            if message.get("mode") == "choreography":
                cmd = {
                    "type": "DeliveryScheduleCommand",
                    "order_id": order_id,
                    "mode": "choreography",
                    "fail_delivery": message.get("fail_delivery", False),
                    "retry_delivery": message.get("retry_delivery", 0)
                }
                publish_message(DELIVERY_TOPIC, cmd)
                print(f"[Order Service] -> DeliveryScheduleCommand (choreography)")

        elif msg_type == "RestaurantFailedEvent":
            order.status = "failed"
            session.commit()
            if message.get("mode") == "choreography":
                cmd = {"type": "RefundPaymentCommand", "order_id": order_id}
                publish_message(PAYMENT_TOPIC, cmd)
                print(f"[Order Service] -> RefundPaymentCommand (choreography)")

        elif msg_type == "DeliveryScheduledEvent":
            order.status = "completed"
            session.commit()

        elif msg_type == "DeliveryFailedEvent":
            order.status = "failed"
            session.commit()
            if message.get("mode") == "choreography":
                cmd1 = {"type": "CancelRestaurantCommand", "order_id": order_id}
                publish_message(RESTAURANT_TOPIC, cmd1)
                cmd2 = {"type": "RefundPaymentCommand", "order_id": order_id}
                publish_message(PAYMENT_TOPIC, cmd2)
                print(f"[Order Service] -> CancelRestaurant & RefundPayment (choreography)")

    session.close()

def start_consumer():
    thread = threading.Thread(target=consume_messages, daemon=True)
    thread.start()

@app.route("/order/create", methods=["POST"])
def create_order():
    data = request.json or {}
    mode = "choreography"  # default
    # Lấy mode nếu có
    if data.get("mode") in ["choreography", "orchestration"]:
        mode = data["mode"]

    fail_payment = data.get("fail_payment", False)
    retry_payment = data.get("retry_payment", 0)
    fail_restaurant = data.get("fail_restaurant", False)
    retry_restaurant = data.get("retry_restaurant", 0)
    fail_delivery = data.get("fail_delivery", False)
    retry_delivery = data.get("retry_delivery", 0)

    session = Session()
    o = Order(status="initiated")
    session.add(o)
    session.commit()
    order_id = o.id
    session.close()

    print(f"[Order Service] Created order {order_id}, mode={mode}")

    if mode == "choreography":
        # Gửi PaymentReserveCommand như cũ
        cmd = {
            "type": "PaymentReserveCommand",
            "order_id": order_id,
            "mode": "choreography",
            "fail_payment": fail_payment,
            "retry_payment": retry_payment,
            "fail_restaurant": fail_restaurant,
            "retry_restaurant": retry_restaurant,
            "fail_delivery": fail_delivery,
            "retry_delivery": retry_delivery
        }
        publish_message(PAYMENT_TOPIC, cmd)
        print(f"[Order Service] -> PaymentReserveCommand (choreography)")
    else:
        # Orchestration => gửi OrderCreatedEvent sang orchestrator_service
        event = {
            "type": "OrderCreatedEvent",
            "order_id": order_id,
            "mode": "orchestration",
            "fail_payment": fail_payment,
            "retry_payment": retry_payment,
            "fail_restaurant": fail_restaurant,
            "retry_restaurant": retry_restaurant,
            "fail_delivery": fail_delivery,
            "retry_delivery": retry_delivery
        }
        publish_message(ORCHESTRATOR_TOPIC, event)
        print(f"[Order Service] -> OrderCreatedEvent (orchestration)")

    return jsonify({"order_id": order_id, "status": "initiated", "mode": mode})

@app.route("/orders", methods=["GET"])
def get_orders():
    session = Session()
    orders = session.query(Order).all()
    result = []
    for o in orders:
        result.append({
            "id": o.id,
            "status": o.status,
            "created_at": o.created_at.isoformat()
        })
    session.close()
    return jsonify(result)

if __name__ == "__main__":
    start_consumer()
    app.run(port=5000, debug=True)
