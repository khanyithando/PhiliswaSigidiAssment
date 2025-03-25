# Financial Data Engineering Take-Home Assignment

This Docker environment provides a pre-configured setup for the take-home assignment with Apache Airflow, PostgreSQL, and FastAPI.

## Getting Started

1. Install Docker and Docker Compose on your machine if you haven't already.
2. Clone or download this repository.
3. Run the Docker containers:

```bash
docker-compose up -d
```

4. Access the services:
   - Airflow webserver: http://localhost:8080 (username: admin, password: admin)
   - FastAPI service: http://localhost:8000 (with Swagger docs at http://localhost:8000/docs)

## Assignment Overview

### Task A: ETL Pipeline using Apache Airflow
Build an ETL pipeline that processes financial transaction data using Apache Airflow.

Your task:
- Implement the ETL DAG in `dags/etl_transactions.py`
- The DAG should:
  - Extract: Load transactions from the provided CSV in `data/financial_transactions.csv` into a Pandas DataFrame
  - Transform:
    - Convert amounts to float (handling formatting like "$4990.00", etc.)
    - Normalize date formats to YYYY-MM-DD (handling various formats like MM/DD/YYYY, DD-MM-YYYY)
    - Remove duplicate transactions
  - Load: Insert the cleaned transactions into the PostgreSQL table `transactions`
- Add appropriate logging and error handling

### Task B: FastAPI Service
Build a FastAPI service that provides an API to access the processed transaction data.

Your task:
- Implement the following endpoint in the FastAPI service:
  - `GET /transactions/{user_id}/summary`: Returns a summary of transactions for a specific user
  - Response should include:
    - total_transactions (count)
    - total_amount (sum)
    - average_transaction_amount
- Use SQLAlchemy with PostgreSQL to retrieve the data
- Implement proper error handling (e.g., 404 if no transactions exist for the user)
- Make the API asynchronous (using async/await)

## Database Information

- PostgreSQL is available at `localhost:5432`
- Database name: `finance_app`
- Username: `airflow`
- Password: `airflow`
- Table schema:
```sql
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    user_id INT NOT NULL,
    amount FLOAT NOT NULL,
    transaction_date DATE NOT NULL
);
```

## Sample Dataset

The provided dataset (`data/financial_transactions.csv`) contains sample financial transactions with the following format:
- transaction_id: Unique ID for each transaction
- user_id: ID of the user who performed the transaction
- amount: Transaction amount (with various formats including "$" prefix, negative values, etc.)
- transaction_date: Date of the transaction (in various formats)
