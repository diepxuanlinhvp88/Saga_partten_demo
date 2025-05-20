import json
import threading, time, datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)
CORS(app)

engine = create_engine('sqlite:///transaction_coordinator.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

class ParticipantStatus(Base):
    __tablename__ = 'participant_statuses'
    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(Integer)
    participant = Column(String)
    status = Column(String)
    vote = Column(Boolean, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

Base.metadata.create_all(engine)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
COORDINATOR_TOPIC = 'transaction_coordinator'
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

def create_transaction(order_id):
    session = Session()
    tx = Transaction(order_id=order_id, status='initiated')
    session.add(tx)
    session.commit()
    tx_id = tx.id

    participants = ['payment', 'restaurant', 'delivery']
    for p in participants:
        ps = ParticipantStatus(transaction_id=tx_id, participant=p, status='preparing', vote=None)
        session.add(ps)

    session.commit()
    session.close()
    return tx_id

def update_transaction_status(tx_id, status):
    session = Session()
    tx = session.query(Transaction).filter_by(id=tx_id).first()
    if tx:
        tx.status = status
        session.commit()
    session.close()

def update_participant_status(tx_id, participant, status, vote=None):
    session = Session()
    ps = session.query(ParticipantStatus).filter_by(transaction_id=tx_id, participant=participant).first()
    if ps:
        ps.status = status
        if vote is not None:
            ps.vote = vote
        session.commit()
    session.close()

def check_all_prepared(tx_id):
    session = Session()
    all_prepared = True
    all_voted_yes = True

    participants = session.query(ParticipantStatus).filter_by(transaction_id=tx_id).all()
    for p in participants:
        if p.status != 'prepared':
            all_prepared = False
            break
        if p.vote is not True:
            all_voted_yes = False
            break

    session.close()
    return all_prepared, all_voted_yes

def consume_messages():
    consumer = KafkaConsumer(
        COORDINATOR_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='coordinator_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for msg in consumer:
        message = msg.value
        msg_type = message.get("type")
        order_id = message.get("order_id")
        tx_id = message.get("transaction_id")

        print(f"[Coordinator] Received {msg_type} for order {order_id}, tx {tx_id}")

        if msg_type == "InitiateTransaction":
            tx_id = create_transaction(order_id)
            update_transaction_status(tx_id, 'preparing')

            prepare_cmd = {
                "type": "PrepareCommand",
                "order_id": order_id,
                "transaction_id": tx_id,
                "mode": "2pc",
                "fail_payment": message.get("fail_payment", False),
                "fail_restaurant": message.get("fail_restaurant", False),
                "fail_delivery": message.get("fail_delivery", False)
            }

            publish_message(PAYMENT_TOPIC, prepare_cmd)
            publish_message(RESTAURANT_TOPIC, prepare_cmd)
            publish_message(DELIVERY_TOPIC, prepare_cmd)
            print(f"[Coordinator] Sent PrepareCommand to all services for tx {tx_id}")

        elif msg_type == "VoteCommit":
            participant = message.get("participant")
            update_participant_status(tx_id, participant, 'prepared', True)

            all_prepared, all_voted_yes = check_all_prepared(tx_id)
            if all_prepared:
                if all_voted_yes:
                    update_transaction_status(tx_id, 'committing')

                    commit_cmd = {
                        "type": "CommitCommand",
                        "order_id": order_id,
                        "transaction_id": tx_id,
                        "mode": "2pc"
                    }

                    publish_message(PAYMENT_TOPIC, commit_cmd)
                    publish_message(RESTAURANT_TOPIC, commit_cmd)
                    publish_message(DELIVERY_TOPIC, commit_cmd)
                    print(f"[Coordinator] Sent CommitCommand to all services for tx {tx_id}")
                else:
                    update_transaction_status(tx_id, 'aborting')

                    abort_cmd = {
                        "type": "AbortCommand",
                        "order_id": order_id,
                        "transaction_id": tx_id,
                        "mode": "2pc"
                    }

                    publish_message(PAYMENT_TOPIC, abort_cmd)
                    publish_message(RESTAURANT_TOPIC, abort_cmd)
                    publish_message(DELIVERY_TOPIC, abort_cmd)
                    print(f"[Coordinator] Sent AbortCommand to all services for tx {tx_id}")

        elif msg_type == "VoteAbort":
            participant = message.get("participant")
            update_participant_status(tx_id, participant, 'prepared', False)

            update_transaction_status(tx_id, 'aborting')

            abort_cmd = {
                "type": "AbortCommand",
                "order_id": order_id,
                "transaction_id": tx_id,
                "mode": "2pc"
            }

            publish_message(PAYMENT_TOPIC, abort_cmd)
            publish_message(RESTAURANT_TOPIC, abort_cmd)
            publish_message(DELIVERY_TOPIC, abort_cmd)
            print(f"[Coordinator] Sent AbortCommand to all services for tx {tx_id}")

        elif msg_type == "CommitComplete":
            participant = message.get("participant")
            update_participant_status(tx_id, participant, 'committed')

            session = Session()
            all_committed = True
            participants = session.query(ParticipantStatus).filter_by(transaction_id=tx_id).all()
            for p in participants:
                if p.status != 'committed':
                    all_committed = False
                    break
            session.close()

            if all_committed:
                update_transaction_status(tx_id, 'committed')
                complete_event = {
                    "type": "TransactionCompletedEvent",
                    "order_id": order_id,
                    "transaction_id": tx_id,
                    "status": "committed",
                    "mode": "2pc"
                }
                publish_message(ORDER_TOPIC, complete_event)
                print(f"[Coordinator] Transaction {tx_id} completed successfully")

        elif msg_type == "AbortComplete":
            participant = message.get("participant")
            update_participant_status(tx_id, participant, 'aborted')

            session = Session()
            all_aborted = True
            participants = session.query(ParticipantStatus).filter_by(transaction_id=tx_id).all()
            for p in participants:
                if p.status != 'aborted' and p.status != 'preparing':
                    all_aborted = False
                    break
            session.close()

            if all_aborted:
                update_transaction_status(tx_id, 'aborted')
                abort_event = {
                    "type": "TransactionAbortedEvent",
                    "order_id": order_id,
                    "transaction_id": tx_id,
                    "mode": "2pc"
                }
                publish_message(ORDER_TOPIC, abort_event)
                print(f"[Coordinator] Transaction {tx_id} aborted")

threading.Thread(target=consume_messages, daemon=True).start()

@app.route("/transactions", methods=["GET"])
def get_transactions():
    session = Session()
    txs = session.query(Transaction).all()
    result = []
    for tx in txs:
        result.append({
            "id": tx.id,
            "order_id": tx.order_id,
            "status": tx.status,
            "created_at": tx.created_at.isoformat(),
            "updated_at": tx.updated_at.isoformat()
        })
    session.close()
    return jsonify(result)

@app.route("/transactions/<int:tx_id>/participants", methods=["GET"])
def get_transaction_participants(tx_id):
    session = Session()
    participants = session.query(ParticipantStatus).filter_by(transaction_id=tx_id).all()
    result = []
    for p in participants:
        result.append({
            "id": p.id,
            "transaction_id": p.transaction_id,
            "participant": p.participant,
            "status": p.status,
            "vote": p.vote,
            "created_at": p.created_at.isoformat(),
            "updated_at": p.updated_at.isoformat()
        })
    session.close()
    return jsonify(result)

if __name__ == "__main__":
    app.run(port=7006, debug=True)
