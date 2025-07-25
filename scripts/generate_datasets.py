import pandas as pd
import json
import random
from faker import Faker

fake = Faker()

# ---- 1. Customers CSV ----
customers = [{
    'customer_id': i,
    'first_name': fake.first_name(),
    'last_name': fake.last_name(),
    'email': fake.email(),
    'country': fake.country(),
    'signup_date': fake.date_between(start_date='-3y', end_date='today')
} for i in range(1, 101)]
pd.DataFrame(customers).to_csv('../data/customers.csv', index=False)

# ---- 2. Transactions Excel ----
transactions = [{
    'InvoiceNo': f'INV{1000+i}',
    'CustomerID': random.randint(1, 100),
    'Quantity': random.randint(1, 10),
    'UnitPrice': round(random.uniform(5.0, 50.0), 2),
    'InvoiceDate': fake.date_between(start_date='-3y', end_date='today')
} for i in range(500)]
pd.DataFrame(transactions).to_excel('../data/transactions.xlsx', index=False)

# ---- 3. Web Logs CSV ----
logs = [{
    'ip': fake.ipv4(),
    'timestamp': fake.iso8601(),
    'method': random.choice(['GET', 'POST']),
    'endpoint': fake.uri_path(),
    'status': random.choice([200, 404, 500]),
    'bytes_sent': random.randint(100, 5000)
} for _ in range(300)]
pd.DataFrame(logs).to_csv('../data/web_logs.csv', index=False)

# ---- 4. Support Tickets JSON ----
tickets = [{
    'ticket_id': f'TK{i}',
    'customer_id': random.randint(1, 100),
    'issue': fake.sentence(),
    'category': random.choice(['Billing', 'Technical', 'Account']),
    'status': random.choice(['open', 'closed', 'in_progress']),
    'created_at': fake.iso8601()
} for i in range(100)]
with open('../data/support_tickets.json', 'w') as f:
    json.dump(tickets, f, indent=2)
