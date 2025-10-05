import json
import sqlite3
from textblob import TextBlob
import time

# Function to process a single message
def process_message(message: dict):
    """
    Process a single JSON message. For example, you can perform sentiment analysis 
    or store the message in a database.

    Args:
    message (dict): The JSON message as a Python dictionary.
    """
    patient_id = message["patient_id"]
    feedback = message["feedback"]
    timestamp = message["timestamp"]

    # Example: Perform Sentiment Analysis using TextBlob
    sentiment = TextBlob(feedback).sentiment.polarity
    sentiment_label = "Positive" if sentiment > 0.1 else "Negative" if sentiment < -0.1 else "Neutral"
    
    # Print processed feedback (you can remove this in production)
    print(f"Processed message for patient {patient_id}")
    print(f"Feedback: {feedback}")
    print(f"Sentiment: {sentiment_label}")

    # Now, let's store this processed data in an SQLite database
    store_in_db(patient_id, feedback, sentiment, sentiment_label, timestamp)

# Function to store the processed message in SQLite
def store_in_db(patient_id: str, feedback: str, sentiment: float, sentiment_label: str, timestamp: str):
    """
    Store processed feedback in an SQLite database.
    """
    db_path = "patient_feedback.db"  # Path to your SQLite DB file
    
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create the table if it does not exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS feedback (
            patient_id TEXT,
            feedback TEXT,
            sentiment REAL,
            sentiment_label TEXT,
            timestamp TEXT
        )
    ''')

    # Insert the processed message data into the database
    cursor.execute('''
        INSERT INTO feedback (patient_id, feedback, sentiment, sentiment_label, timestamp)
        VALUES (?, ?, ?, ?, ?)
    ''', (patient_id, feedback, sentiment, sentiment_label, timestamp))
    
    conn.commit()  # Commit the transaction
    conn.close()   # Close the connection
    print(f"Stored message for patient {patient_id} into database.")

# Main consumer logic to read messages from the file
def consume_messages_from_file(file_path: str):
    """
    Reads messages from a dynamic file (patient_feedback.jsonl) and processes each one.
    """
    print(f"Starting to consume messages from {file_path}")
    while True:
        try:
            # Open the file in append mode to keep reading new lines
            with open(file_path, "r") as file:
                # Read the file line by line
                lines = file.readlines()
                for line in lines:
                    try:
                        # Parse the JSON line to a dictionary
                        message = json.loads(line.strip())
                        print(f"Processing message: {message}")
                        
                        # Call the function to process and store the message
                        process_message(message)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}")
                    except Exception as e:
                        print(f"Error processing message: {e}")
            
            # Sleep for a few seconds to avoid constant polling of the file
            print("Waiting for new messages...")
            time.sleep(5)  # Adjust the sleep time as per your requirements
        
        except Exception as e:
            print(f"Error reading the file: {e}")
            time.sleep(5)  # Sleep before retrying if an error occurs

if __name__ == "__main__":
    # Path to the dynamic file (patient_feedback.jsonl)
    file_path = "data/patient_feedback.jsonl"
    
    # Call the consumer function to start reading and processing messages
    consume_messages_from_file(file_path)
