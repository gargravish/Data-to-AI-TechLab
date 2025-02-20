# Import required libraries
import os
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud import aiplatform
import json
from datetime import datetime
import time
from decimal import Decimal

# Authentication (run this if not already authenticated)
from google.colab import auth
auth.authenticate_user()

# Configuration
PROJECT_ID = ""
SUBSCRIPTION_PATH = "projects/{PROJECT_ID}/subscriptions/ff-tx-sub"
LOCATION = "us-central1"
DATASET_ID = "tx"
BQ_TABLE = f"{PROJECT_ID}.{DATASET_ID}.online_fraud_prediction"
ENDPOINT_ID = ""

# Initialize clients
subscriber = pubsub_v1.SubscriberClient()
bq_client = bigquery.Client(project=PROJECT_ID)
aiplatform.init(project=PROJECT_ID, location=LOCATION)

def convert_decimal_to_float(obj):
    """Convert Decimal objects to float"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimal_to_float(v) for k, v in obj.items()}
    return obj

class FraudDetectionProcessor:
    def __init__(self):
        """Initialize the processor with necessary clients and configurations"""
        self.endpoint = aiplatform.Endpoint(
            endpoint_name=f"projects/{PROJECT_ID}/locations/{LOCATION}/endpoints/{ENDPOINT_ID}"  
        )

    def get_features(self, transaction_data):
        """
        Get features for the transaction by querying BigQuery
        Args:
            transaction_data: dict containing transaction information
        Returns:
            dict of features
        """
        # Query to get customer features (both batch and streaming)
        customer_query = f"""
        WITH batch_features AS (
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.batch_customer_features`
            WHERE CUSTOMER_ID = @customer_id
        ),
        stream_features AS (
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.streaming_customer_features`
            WHERE CUSTOMER_ID = @customer_id
        )
        SELECT
            COALESCE(b.nb_tx_1day, 0) as customer_nb_tx_1day,
            COALESCE(b.avg_amount_1day, 0) as customer_avg_amount_1day,
            COALESCE(b.nb_tx_7day, 0) as customer_nb_tx_7day,
            COALESCE(b.avg_amount_7day, 0) as customer_avg_amount_7day,
            COALESCE(b.nb_tx_15day, 0) as customer_nb_tx_15day,
            COALESCE(b.avg_amount_15day, 0) as customer_avg_amount_15day,
            COALESCE(s.nb_tx_15min, 0) as customer_nb_tx_15min,
            COALESCE(s.avg_amount_15min, 0) as customer_avg_amount_15min,
            COALESCE(s.nb_tx_30min, 0) as customer_nb_tx_30min,
            COALESCE(s.avg_amount_30min, 0) as customer_avg_amount_30min,
            COALESCE(s.nb_tx_60min, 0) as customer_nb_tx_60min,
            COALESCE(s.avg_amount_60min, 0) as customer_avg_amount_60min
        FROM batch_features b
        FULL OUTER JOIN stream_features s
        ON b.CUSTOMER_ID = s.CUSTOMER_ID
        """

        # Query to get terminal features (both batch and streaming)
        terminal_query = f"""
        WITH batch_features AS (
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.batch_terminal_features`
            WHERE TERMINAL_ID = @terminal_id
        ),
        stream_features AS (
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.streaming_terminal_features`
            WHERE TERMINAL_ID = @terminal_id
        )
        SELECT
            COALESCE(b.risk_1day, 0) as terminal_risk_1day,
            COALESCE(b.nb_tx_1day, 0) as terminal_nb_tx_1day,
            COALESCE(b.risk_7day, 0) as terminal_risk_7day,
            COALESCE(b.nb_tx_7day, 0) as terminal_nb_tx_7day,
            COALESCE(b.risk_15day, 0) as terminal_risk_15day,
            COALESCE(b.nb_tx_15day, 0) as terminal_nb_tx_15day,
            COALESCE(s.risk_15min, 0) as terminal_risk_15min,
            COALESCE(s.nb_tx_15min, 0) as terminal_nb_tx_15min,
            COALESCE(s.risk_30min, 0) as terminal_risk_30min,
            COALESCE(s.nb_tx_30min, 0) as terminal_nb_tx_30min,
            COALESCE(s.risk_60min, 0) as terminal_risk_60min,
            COALESCE(s.nb_tx_60min, 0) as terminal_nb_tx_60min
        FROM batch_features b
        FULL OUTER JOIN stream_features s
        ON b.TERMINAL_ID = s.TERMINAL_ID
        """

        # Execute queries with parameters
        customer_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("customer_id", "STRING", transaction_data['CUSTOMER_ID'])
            ]
        )
        terminal_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("terminal_id", "STRING", transaction_data['TERMINAL_ID'])
            ]
        )

        customer_features = bq_client.query(customer_query, job_config=customer_job_config).to_dataframe()
        terminal_features = bq_client.query(terminal_query, job_config=terminal_job_config).to_dataframe()

        # Initialize features dictionary with transaction data
        features = {
            'TX_AMOUNT': float(transaction_data['TX_AMOUNT']),
            'TERMINAL_ID': str(transaction_data['TERMINAL_ID'])
        }

        # Add customer features
        if not customer_features.empty:
            for col in customer_features.columns:
                features[col] = float(customer_features.iloc[0][col])

        # Add terminal features
        if not terminal_features.empty:
            for col in terminal_features.columns:
                features[col] = float(terminal_features.iloc[0][col])

        return features

    def extract_prediction_probability(self, prediction_response):
        """
        Extract the fraud probability from the prediction response
        Args:
            prediction_response: Response from Vertex AI endpoint
        Returns:
            float: Probability of fraud
        """
        pred_value = prediction_response.predictions[0]

        # Print the prediction response structure for debugging
        print(f"Prediction response structure: {pred_value}")

        if isinstance(pred_value, dict):
            # Handle the specific response format we're getting
            if 'TX_FRAUD_probs' in pred_value:
                # Get the probability of fraud (second value in the array)
                return float(pred_value['TX_FRAUD_probs'][0])  # Using first value as fraud probability
            elif 'scores' in pred_value:
                return float(pred_value['scores'][1])
            elif 'probability' in pred_value:
                return float(pred_value['probability'])
            elif 'classes' in pred_value:
                class_probs = pred_value.get('probabilities', [0, 0])
                return float(class_probs[1])
        elif isinstance(pred_value, (list, tuple)):
            return float(pred_value[1] if len(pred_value) > 1 else pred_value[0])
        else:
            return float(pred_value)

    def save_prediction(self, prediction_data):
        """
        Save prediction to BigQuery
        Args:
            prediction_data: dict containing prediction results
        """
        # Convert datetime to ISO format string
        current_time = datetime.utcnow().isoformat()

        rows_to_insert = [{
            'TX_ID': prediction_data['TX_ID'],
            'prediction_timestamp': current_time,  # Use ISO format string
            'fraud_probability': float(prediction_data['fraud_probability']),
            'is_fraud': bool(float(prediction_data['fraud_probability']) > 0.5),
            'model_version': prediction_data['model_version']
        }]

        try:
            errors = bq_client.insert_rows_json(BQ_TABLE, rows_to_insert)
            if errors:
                print(f"Encountered errors while inserting rows: {errors}")
            else:
                print(f"Successfully inserted prediction for TX_ID: {prediction_data['TX_ID']}")
        except Exception as e:
            print(f"Error inserting into BigQuery: {str(e)}")
            print(f"Attempted to insert: {json.dumps(rows_to_insert, indent=2)}")
            raise

    def process_message(self, message):
        """
        Process a single Pub/Sub message
        Args:
            message: Pub/Sub message
        """
        try:
            # Decode message data
            message_data = json.loads(message.data.decode('utf-8'))
            print(f"Processing transaction {message_data['TX_ID']}")

            # Get features
            features = self.get_features(message_data)

            # Debug print
            print(f"Features prepared: {json.dumps(features, indent=2)}")

            # Make prediction
            prediction = self.endpoint.predict(
                instances=[features]
            )

            # Extract prediction probability
            try:
                fraud_prob = self.extract_prediction_probability(prediction)
                print(f"Extracted fraud probability: {fraud_prob}")
            except Exception as e:
                print(f"Error extracting probability: {str(e)}")
                print(f"Full prediction response: {prediction}")
                raise

            # Prepare prediction data
            prediction_data = {
                'TX_ID': message_data['TX_ID'],
                'fraud_probability': fraud_prob,
                'model_version': getattr(prediction, 'deployed_model_id', 'unknown')
            }

            # Debug print
            print(f"Prediction result: {json.dumps(prediction_data, indent=2)}")

            # Save prediction
            self.save_prediction(prediction_data)

            # Acknowledge the message
            message.ack()

            print(f"Successfully processed transaction {message_data['TX_ID']}")

        except Exception as e:
            print(f"Error processing message: {str(e)}")
            print(f"Message data: {message.data.decode('utf-8')}")
            if 'prediction' in locals():
                print(f"Prediction response: {prediction}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            # Nack the message so it can be retried
            message.nack()

def main():
    """Main function to run the processor"""
    processor = FraudDetectionProcessor()

    # Callback function for received messages
    def callback(message):
        processor.process_message(message)

    # Start subscribing to messages
    streaming_pull_future = subscriber.subscribe(
        SUBSCRIPTION_PATH, callback=callback
    )
    print(f"Listening for messages on {SUBSCRIPTION_PATH}")

    try:
        # Keep the main thread alive
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        subscriber.close()

# Run the main function
if __name__ == "__main__":
    main()