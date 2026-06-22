from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

DATA_FILE = "/opt/airflow/data/customers.csv"
OUTPUT_DIR = "/opt/airflow/output"


def extract_customers(**context):
    df = pd.read_csv(DATA_FILE)

    print("=== Customers Extracted ===")
    print(df)

    context["ti"].xcom_push(
        key="customers",
        value=df.to_dict("records")
    )


def validate_customers(**context):
    customers = context["ti"].xcom_pull(
        task_ids="extract_customers",
        key="customers"
    )

    valid = []

    for customer in customers:
        if customer.get("name") and customer.get("email"):
            valid.append(customer)

    print(f"Valid Customers: {len(valid)}")

    context["ti"].xcom_push(
        key="valid_customers",
        value=valid
    )


def load_database(**context):
    customers = context["ti"].xcom_pull(
        task_ids="validate_customers",
        key="valid_customers"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(
        os.path.join(OUTPUT_DIR, "customers_loaded.txt"),
        "w"
    ) as f:

        for customer in customers:
            f.write(
                f"Loaded Customer {customer['customer_id']}\n"
            )

    print("Database load complete")


def send_welcome_email(**context):
    customers = context["ti"].xcom_pull(
        task_ids="extract_customers",
        key="customers"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(
        os.path.join(OUTPUT_DIR, "emails_sent.txt"),
        "w"
    ) as f:

        for customer in customers:
            f.write(
                f"Email sent to {customer['email']}\n"
            )

    print("Emails sent successfully")


with DAG(
    dag_id="customer_onboarding",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers
    )

    validate_task = PythonOperator(
        task_id="validate_customers",
        python_callable=validate_customers
    )

    load_task = PythonOperator(
        task_id="load_database",
        python_callable=load_database
    )

    email_task = PythonOperator(
        task_id="send_welcome_email",
        python_callable=send_welcome_email
    )

    extract_task >> validate_task >> load_task
    extract_task >> email_task



