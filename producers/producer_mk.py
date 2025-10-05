import json
import random
import time
from datetime import datetime

# Path to save the dynamic file
FILE_PATH = 'data/patient_feedback.jsonl'

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
    
    # Write the message to the dynamic file
    with open(FILE_PATH, 'a') as f:
        f.write(json.dumps(message) + "\n")
    print(f"Data written: {message}")

# Simulate writing data to the file every 10 seconds
while True:
    generate_data()
    time.sleep(10)
