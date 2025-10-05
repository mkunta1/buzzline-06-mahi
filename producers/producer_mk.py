import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Kafka configuration
KAFKA_SERVER = 'localhost:9092'  # Replace with your Kafka server address
TOPIC_NAME = 'patient_feedback'  # Kafka topic name where messages will be sent

# Sample feedback messages to simulate real-time patient feedback
feedback_list = [
    "I feel great after the treatment.",
    "Still feeling some pain.",
    "My condition has improved significantly.",
    "I'm not feeling any better.",
    "The treatment worked wonders for me."
]

# Function to generate mock data
def generate_data():
    patient_id = random.randint(10000, 99999)  # Random patient ID
    feedback = random.choice(feedback_list)  # Random feedback message
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Timestamp for the message
    
    # Create the JSON message
    message = {
        "patient_id": str(patient_id),
        "feedback": feedback,
        "timestamp": timestamp
    }
    return message

# Function to send data to Kafka
def send_to_kafka(producer, message):
    # Send the message to Kafka topic
    producer.send(TOPIC_NAME, value=message)
    producer.flush()  # Ensure message is sent
    print(f"Sent message: {message}")

# Main function to generate and send data to Kafka
def main():
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON
    )
    
    while True:
        message = generate_data()  # Generate mock data
        send_to_kafka(producer, message)  # Send the data to Kafka
        time.sleep(15)  # Simulate a 30-second interval between data generation

if __name__ == "__main__":
    main()
