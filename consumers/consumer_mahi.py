import json
import sqlite3
import time
import matplotlib.pyplot as plt
from textblob import TextBlob
from collections import deque
from kafka import KafkaConsumer

# -------------------------------------------------------
# Configuration
# -------------------------------------------------------
MAX_DATA_POINTS = 10               # Number of patients shown on chart
HIGH_RISK_THRESHOLD = -0.5         # Sentiment threshold for high-risk patients
UPDATE_INTERVAL = 10               # ⏱ Chart update every 20 seconds
KAFKA_TOPIC = 'patient_feedback'   # Kafka topic name
KAFKA_SERVER = 'localhost:9092'    # Kafka broker address
GROUP_ID = 'sentiment_group_1'     # Consumer group ID

# -------------------------------------------------------
# Deques for recent data (acts like a rolling window)
# -------------------------------------------------------
sentiment_data = deque(maxlen=MAX_DATA_POINTS)
patient_ids = deque(maxlen=MAX_DATA_POINTS)
feedback_reasons = deque(maxlen=MAX_DATA_POINTS)

# -------------------------------------------------------
# Process a single incoming message
# -------------------------------------------------------
def process_message(message: dict):
    patient_id = message["patient_id"]
    feedback = message["feedback"]
    timestamp = message["timestamp"]

    # Sentiment analysis
    sentiment = TextBlob(feedback).sentiment.polarity
    sentiment_label = "Positive" if sentiment > 0.1 else "Negative" if sentiment < -0.1 else "Neutral"

    # Store to DB
    store_in_db(patient_id, feedback, sentiment, sentiment_label, timestamp)

    # Identify high-risk cases
    if sentiment < HIGH_RISK_THRESHOLD:
        reason = identify_risk_reason(feedback)
        print(f"⚠️  Patient {patient_id} is HIGH RISK → {reason}")
        feedback_reasons.append(reason)
    else:
        feedback_reasons.append("Normal")

    # Update visualization
    update_chart(sentiment, patient_id)

    # Console log
    print(f"Processed: {patient_id} | {feedback} | Sentiment: {sentiment_label}")

# -------------------------------------------------------
# Save processed data to SQLite
# -------------------------------------------------------
def store_in_db(patient_id, feedback, sentiment, sentiment_label, timestamp):
    conn = sqlite3.connect("patient_feedback.db")
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS feedback (
        patient_id TEXT,
        feedback TEXT,
        sentiment REAL,
        sentiment_label TEXT,
        timestamp TEXT
    )''')
    cursor.execute('INSERT INTO feedback VALUES (?, ?, ?, ?, ?)',
                   (patient_id, feedback, sentiment, sentiment_label, timestamp))
    conn.commit()
    conn.close()

# -------------------------------------------------------
# Identify risk reason from keywords
# -------------------------------------------------------
def identify_risk_reason(feedback: str):
    fb = feedback.lower()
    if any(word in fb for word in ["pain", "worst", "terrible"]):
        return "Severe Pain"
    if any(word in fb for word in ["no improvement", "not better"]):
        return "No Improvement"
    if any(word in fb for word in ["still feel awful", "nothing works"]):
        return "Ineffective Treatment"
    return "General Risk"

# -------------------------------------------------------
# Real-time chart update
# -------------------------------------------------------
def update_chart(sentiment, patient_id):
    sentiment_data.append(sentiment)
    patient_ids.append(patient_id)

    plt.clf()
    plt.plot(patient_ids, sentiment_data, label="Sentiment Trend", color="blue")
    plt.scatter(patient_ids, sentiment_data,
                color=["red" if s < HIGH_RISK_THRESHOLD else "blue" for s in sentiment_data],
                zorder=5)
    plt.xlabel("Patient ID")
    plt.ylabel("Sentiment Polarity")
    plt.title("Real-Time Patient Feedback Sentiment")
    plt.xticks(rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()
    plt.pause(0.1)  # allow the chart to refresh

# -------------------------------------------------------
# Consume messages from Kafka
# -------------------------------------------------------
def consume_messages_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",      # ✅ Start from newest messages only
        enable_auto_commit=True,
        group_id=GROUP_ID
    )

    plt.ion()
    plt.figure(figsize=(10, 6))
    print("✅ Consumer started — waiting for new messages...")

    for msg in consumer:
        try:
            process_message(msg.value)
            time.sleep(UPDATE_INTERVAL)  # ⏱ chart updates every 20 seconds
        except Exception as e:
            print(f"Error processing message: {e}")

# -------------------------------------------------------
# Run
# -------------------------------------------------------
if __name__ == "__main__":
    consume_messages_from_kafka()
