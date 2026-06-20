from kafka import KafkaConsumer
import json

def user_login_and_listen():
    print("=== Fraud Alert System ===")
    user_id_input = input("Enter your userId to login: ")

    try:
        user_id = int(user_id_input)
    except:
        print("Invalid id!")
        return

    print(f"Login as userId: {user_id} .. Listening for alerts.......")

    consumer = KafkaConsumer(
        'fraud-notification',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for msg in consumer:
        alert_data = msg.value

        if alert_data.get("userId") == user_id:
            print("\nTransaction Alert:")
            print(f"Name: {alert_data.get('name')}")
            print(f"Tx ID: {alert_data.get('tx_id')}")
            print(f"Amount: ${alert_data.get('amount'):.2f}\n")

if __name__ == "__main__":
    user_login_and_listen()