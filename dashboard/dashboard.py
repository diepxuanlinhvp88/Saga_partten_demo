import json
import threading, time, datetime
import requests
from kafka import KafkaConsumer
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

app = Flask(__name__)
CORS(app)

engine = create_engine('sqlite:///dashboard.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()

class KafkaLog(Base):
    __tablename__ = 'kafka_logs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String)
    partition = Column(Integer)
    offset = Column(Integer)
    message = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(engine)

orchestrator_engine = create_engine('sqlite:///orchestrator.db')
OrchSession = sessionmaker(bind=orchestrator_engine)
OrchBase = declarative_base()

class OrchestratorLog(OrchBase):
    __tablename__ = 'orchestrator_logs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String)
    order_id = Column(Integer)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

OrchBase.metadata.create_all(orchestrator_engine)

coordinator_engine = create_engine('sqlite:///transaction_coordinator.db')
CoordSession = sessionmaker(bind=coordinator_engine)
CoordBase = declarative_base()

class Transaction(CoordBase):
    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

class ParticipantStatus(CoordBase):
    __tablename__ = 'participant_statuses'
    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(Integer)
    participant = Column(String)
    status = Column(String)
    vote = Column(Boolean, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

CoordBase.metadata.create_all(coordinator_engine)

demo_settings = {
    "mode": "choreography",
    "error_simulation": {
        "order_service": False,
        "payment_service": False,
        "restaurant_service": False,
        "delivery_service": False
    },
    "retry_counts": {
        "order_service": 0,
        "payment_service": 0,
        "restaurant_service": 0,
        "delivery_service": 0
    }
}

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPICS = ['order_service', 'payment_service', 'restaurant_service', 'delivery_service', 'orchestrator_service', 'transaction_coordinator']

def kafka_log_consumer():
    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='dashboard_log_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    session = Session()
    for msg in consumer:
        log_record = KafkaLog(
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
            message=json.dumps(msg.value)
        )
        session.add(log_record)
        session.commit()
        print(f"[Dashboard] {msg.topic} p{msg.partition} o{msg.offset} => {msg.value}")

threading.Thread(target=kafka_log_consumer, daemon=True).start()

@app.route("/")
@app.route("/home")
def home():
    session = Session()
    logs = session.query(KafkaLog).order_by(KafkaLog.id.asc()).all()
    session.close()
    return render_template("home.html", logs=logs)

@app.route("/order_service")
def order_service():
    orders = []
    try:
        resp = requests.get("http://localhost:5000/orders", timeout=3)
        if resp.status_code == 200:
            orders = resp.json()
    except Exception as e:
        print("[Dashboard] Error fetching orders:", e)
    return render_template("order_service.html", orders=orders)

@app.route("/payment_service")
def payment_service():
    payments = []
    try:
        resp = requests.get("http://localhost:6001/payments", timeout=3)
        if resp.status_code == 200:
            payments = resp.json()
    except Exception as e:
        print("[Dashboard] Error fetching payments:", e)
    return render_template("payment_service.html", payments=payments)

@app.route("/restaurant_service")
def restaurant_service():
    restaurant_orders = []
    try:
        resp = requests.get("http://localhost:6002/restaurant_orders", timeout=3)
        if resp.status_code == 200:
            restaurant_orders = resp.json()
    except Exception as e:
        print("[Dashboard] Error fetching restaurant orders:", e)
    return render_template("restaurant_service.html", restaurant_orders=restaurant_orders)

@app.route("/delivery_service")
def delivery_service():
    deliveries = []
    try:
        resp = requests.get("http://localhost:6003/deliveries", timeout=3)
        if resp.status_code == 200:
            deliveries = resp.json()
    except Exception as e:
        print("[Dashboard] Error fetching deliveries:", e)
    return render_template("delivery_service.html", deliveries=deliveries)

@app.route("/setup", methods=["GET", "POST"])
def setup():
    message = ""
    if request.method == "POST":
        mode = request.form.get("mode")
        demo_settings["mode"] = mode
        for svc in ["order_service", "payment_service", "restaurant_service", "delivery_service"]:
            demo_settings["error_simulation"][svc] = (request.form.get(f"error_{svc}") == "on")
            try:
                val = int(request.form.get(f"retry_{svc}", 0))
            except ValueError:
                val = 0
            demo_settings["retry_counts"][svc] = val
        message = "Configuration updated!"
    return render_template("setup.html", settings=demo_settings, message=message)

@app.route("/api/get_setup", methods=["GET"])
def get_setup():
    return jsonify(demo_settings)

@app.route("/api/logs", methods=["GET"])
def api_logs():
    session = Session()
    logs = session.query(KafkaLog).order_by(KafkaLog.id.asc()).all()
    data = []
    for log in logs:
        data.append({
            "id": log.id,
            "topic": log.topic,
            "partition": log.partition,
            "offset": log.offset,
            "message": log.message,
            "created_at": log.created_at.isoformat()
        })
    session.close()
    return jsonify(data)

@app.route("/orchestrator_logs")
def orchestrator_logs():
    session = OrchSession()
    logs = session.query(OrchestratorLog).order_by(OrchestratorLog.id.asc()).all()
    session.close()
    return render_template("orchestrator_logs.html", logs=logs)

@app.route("/transaction_coordinator")
def transaction_coordinator():
    transactions = []
    try:
        resp = requests.get("http://localhost:7006/transactions", timeout=3)
        if resp.status_code == 200:
            transactions = resp.json()
    except Exception as e:
        print("[Dashboard] Error fetching transactions:", e)
    return render_template("transaction_coordinator.html", transactions=transactions)



@app.route("/transaction_coordinator/<int:tx_id>/participants")
def transaction_participants(tx_id):
    participants = []
    try:
        resp = requests.get(f"http://localhost:7006/transactions/{tx_id}/participants", timeout=3)
        if resp.status_code == 200:
            participants = resp.json()
    except Exception as e:
        print("[Dashboard] Error fetching transaction participants:", e)
    return render_template("transaction_participants.html", participants=participants, tx_id=tx_id)

if __name__ == "__main__":
    app.run(port=7000, debug=True)
