@echo off
pip install werkzeug==2.2.2 Flask==2.2.2 flask-cors==3.0.10 SQLAlchemy==1.4.36 kafka-python==2.0.2 requests==2.28.1 six>=1.10.0

echo Khoi dong cac service...
start cmd /k python dashboard/dashboard.py
start cmd /k python orchestrator/orchestrator_service.py
start cmd /k python transaction_coordinator/transaction_coordinator_service.py
start cmd /k python restaurant/restaurant_service.py
start cmd /k python payment/payment_service.py
start cmd /k python delivery/delivery_service.py
start cmd /k python order/order_service.py
