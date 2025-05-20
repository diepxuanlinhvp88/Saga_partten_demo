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
engine = create_engine('sqlite:///payment.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Payment(Base):
    __tablename__ = 'payments'
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(engine)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
PAYMENT_TOPIC = 'payment_service'
ORDER_TOPIC = 'order_service'
ORCHESTRATOR_TOPIC = 'orchestrator_service'
COORDINATOR_TOPIC = 'transaction_coordinator'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def publish_message(topic, message):
    producer.send(topic, message)
    producer.flush()

def consume_messages():
    consumer = KafkaConsumer(
        PAYMENT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='payment_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    session = Session()
    for msg in consumer:
        message = msg.value
        msg_type = message.get("type")
        order_id = message.get("order_id")
        print(f"[Payment Service] Received {msg_type} for order {order_id}")

        pay = Payment(order_id=order_id, status="")
        fail_payment = message.get("fail_payment", False)
        retry_payment = message.get("retry_payment", 0)
        mode = message.get("mode", "choreography")

        if msg_type == "PaymentReserveCommand":
            if fail_payment:
                if retry_payment > 0:
                    new_retry = retry_payment - 1
                    time.sleep(1)
                    cmd = {
                        "type": "PaymentReserveCommand",
                        "order_id": order_id,
                        "mode": mode,
                        "fail_payment": True,
                        "retry_payment": new_retry,
                        "fail_restaurant": message.get("fail_restaurant", False),
                        "retry_restaurant": message.get("retry_restaurant", 0),
                        "fail_delivery": message.get("fail_delivery", False),
                        "retry_delivery": message.get("retry_delivery", 0)
                    }
                    publish_message(PAYMENT_TOPIC, cmd)
                    print(f"[Payment] Retry PaymentReserveCommand (order {order_id}), remain: {new_retry}")
                else:
                    pay.status = "failed"
                    session.add(pay)
                    session.commit()
                    event = {"type": "PaymentFailedEvent", "order_id": order_id}
                    if mode == "choreography":
                        publish_message(ORDER_TOPIC, event)
                    else:
                        publish_message(ORCHESTRATOR_TOPIC, event)
                    print(f"[Payment] PaymentFailedEvent => {mode} for order {order_id}")
            else:
                pay.status = "reserved"
                session.add(pay)
                session.commit()
                event = {
                    "type": "PaymentReservedEvent",
                    "order_id": order_id,
                    "mode": mode,
                    "fail_restaurant": message.get("fail_restaurant", False),
                    "retry_restaurant": message.get("retry_restaurant", 0),
                    "fail_delivery": message.get("fail_delivery", False),
                    "retry_delivery": message.get("retry_delivery", 0)
                }
                if mode == "choreography":
                    publish_message(ORDER_TOPIC, event)
                else:
                    publish_message(ORCHESTRATOR_TOPIC, event)
                print(f"[Payment] PaymentReservedEvent => {mode} (order {order_id})")

        elif msg_type == "RefundPaymentCommand":
            pay.status = "refunded"
            session.add(pay)
            session.commit()
            print(f"[Payment] Payment refunded (order {order_id})")

        elif msg_type == "PrepareCommand":
            tx_id = message.get("transaction_id")
            if fail_payment:
                pay.status = "prepare_failed"
                session.add(pay)
                session.commit()
                vote = {
                    "type": "VoteAbort",
                    "order_id": order_id,
                    "transaction_id": tx_id,
                    "participant": "payment",
                    "mode": "2pc"
                }
                publish_message(COORDINATOR_TOPIC, vote)
                print(f"[Payment] VoteAbort for tx {tx_id} (order {order_id})")
            else:
                pay.status = "prepared"
                session.add(pay)
                session.commit()
                vote = {
                    "type": "VoteCommit",
                    "order_id": order_id,
                    "transaction_id": tx_id,
                    "participant": "payment",
                    "mode": "2pc"
                }
                publish_message(COORDINATOR_TOPIC, vote)
                print(f"[Payment] VoteCommit for tx {tx_id} (order {order_id})")

        elif msg_type == "CommitCommand":
            tx_id = message.get("transaction_id")
            pay.status = "committed"
            session.add(pay)
            session.commit()
            complete = {
                "type": "CommitComplete",
                "order_id": order_id,
                "transaction_id": tx_id,
                "participant": "payment",
                "mode": "2pc"
            }
            publish_message(COORDINATOR_TOPIC, complete)
            print(f"[Payment] CommitComplete for tx {tx_id} (order {order_id})")

        elif msg_type == "AbortCommand":
            tx_id = message.get("transaction_id")
            pay.status = "aborted"
            session.add(pay)
            session.commit()
            complete = {
                "type": "AbortComplete",
                "order_id": order_id,
                "transaction_id": tx_id,
                "participant": "payment",
                "mode": "2pc"
            }
            publish_message(COORDINATOR_TOPIC, complete)
            print(f"[Payment] AbortComplete for tx {tx_id} (order {order_id})")

    session.close()

def start_consumer():
    thread = threading.Thread(target=consume_messages, daemon=True)
    thread.start()

@app.route("/payments", methods=["GET"])
def get_payments():
    session = Session()
    pays = session.query(Payment).all()
    result = []
    for p in pays:
        result.append({
            "id": p.id,
            "order_id": p.order_id,
            "status": p.status,
            "created_at": p.created_at.isoformat()
        })
    session.close()
    return jsonify(result)

if __name__ == "__main__":
    start_consumer()
    app.run(port=6001, debug=True)
