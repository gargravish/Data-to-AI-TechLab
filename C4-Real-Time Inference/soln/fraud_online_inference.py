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
PROJECT_ID = "raves-altostrat"
SUBSCRIPTION_PATH = "projects/raves-altostrat/subscriptions/ff-tx-sub"
LOCATION = "us-central1"
ENDPOINT_ID = "3040498195986644992"
DATASET_ID = "tx"
BQ_TABLE = f"{PROJECT_ID}.{DATASET_ID}.online_fraud_prediction"

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
    def __init__(self, project_id, endpoint_id, location):
            self.project_id = project_id
            self.location = location

            # Initialize Vertex AI
            aiplatform.init(project=project_id, location=location)
            self.endpoint = aiplatform.Endpoint(endpoint_id)

            # Initialize BigQuery client
            self.bq_client = bigquery.Client()

            # Get the table schema
            self.table = self.bq_client.get_table(BQ_TABLE)

    def get_features(self, transaction_data):
        """
        Get features for the transaction by querying BigQuery with column names
        matching exactly what the model expects
        Args:
            transaction_data: dict containing transaction information
        Returns:
            dict of features ready to send to the model
        """
        features_query = f"""
        WITH transaction_data AS (
            SELECT
                @tx_amount as tx_amount,
                @terminal_id as terminal_id,
                @customer_id as customer_id,
                TIMESTAMP(@tx_ts) as tx_timestamp
        ),
        customer_features AS (
            SELECT
                customer_id,
                feature_ts,
                customer_id_nb_tx_15min_window,
                customer_id_avg_amount_15min_window,
                customer_id_nb_tx_30min_window,
                customer_id_avg_amount_30min_window,
                customer_id_nb_tx_60min_window,
                customer_id_avg_amount_60min_window,
                customer_id_nb_tx_1day_window,
                customer_id_avg_amount_1day_window,
                customer_id_nb_tx_7day_window,
                customer_id_avg_amount_7day_window,
                customer_id_nb_tx_14day_window,
                customer_id_avg_amount_14day_window,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY feature_ts DESC) as rn
            FROM `{PROJECT_ID}.{DATASET_ID}.customer_spending_features`
            WHERE customer_id = @customer_id
            AND feature_ts <= (SELECT tx_timestamp FROM transaction_data)
        ),
        terminal_features AS (
            SELECT
                terminal_id,
                feature_ts,
                terminal_id_nb_tx_1day_window,
                terminal_id_risk_1day_window,
                terminal_id_nb_tx_7day_window,
                terminal_id_risk_7day_window,
                terminal_id_nb_tx_14day_window,
                terminal_id_risk_14day_window,
                ROW_NUMBER() OVER (PARTITION BY terminal_id ORDER BY feature_ts DESC) as rn
            FROM `{PROJECT_ID}.{DATASET_ID}.terminal_risk_features`
            WHERE terminal_id = @terminal_id
            AND feature_ts <= (SELECT tx_timestamp FROM transaction_data)
        )
        SELECT
            /* Use the transaction amount directly with the model's expected name */
            d.tx_amount,

            /* Customer features - use the exact column names from the training data */
            COALESCE(c.customer_id_nb_tx_15min_window, 0) as customer_id_nb_tx_15min_window,
            COALESCE(c.customer_id_avg_amount_15min_window, 0) as customer_id_avg_amount_15min_window,
            COALESCE(c.customer_id_nb_tx_30min_window, 0) as customer_id_nb_tx_30min_window,
            COALESCE(c.customer_id_avg_amount_30min_window, 0) as customer_id_avg_amount_30min_window,
            COALESCE(c.customer_id_nb_tx_60min_window, 0) as customer_id_nb_tx_60min_window,
            COALESCE(c.customer_id_avg_amount_60min_window, 0) as customer_id_avg_amount_60min_window,
            COALESCE(c.customer_id_nb_tx_1day_window, 0) as customer_id_nb_tx_1day_window,
            COALESCE(c.customer_id_avg_amount_1day_window, 0) as customer_id_avg_amount_1day_window,
            COALESCE(c.customer_id_nb_tx_7day_window, 0) as customer_id_nb_tx_7day_window,
            COALESCE(c.customer_id_avg_amount_7day_window, 0) as customer_id_avg_amount_7day_window,
            COALESCE(c.customer_id_nb_tx_14day_window, 0) as customer_id_nb_tx_14day_window,
            COALESCE(c.customer_id_avg_amount_14day_window, 0) as customer_id_avg_amount_14day_window,

            /* Terminal features with original column names */
            COALESCE(t.terminal_id_nb_tx_1day_window, 0) as terminal_id_nb_tx_1day_window,
            COALESCE(t.terminal_id_risk_1day_window, 0) as terminal_id_risk_1day_window,
            COALESCE(t.terminal_id_nb_tx_7day_window, 0) as terminal_id_nb_tx_7day_window,
            COALESCE(t.terminal_id_risk_7day_window, 0) as terminal_id_risk_7day_window,
            COALESCE(t.terminal_id_nb_tx_14day_window, 0) as terminal_id_nb_tx_14day_window,
            COALESCE(t.terminal_id_risk_14day_window, 0) as terminal_id_risk_14day_window,

            /* Additional terminal features for shorter windows */
            0.0 as terminal_id_nb_tx_15min_window,
            0.0 as terminal_id_risk_15min_window,
            0.0 as terminal_id_nb_tx_30min_window,
            0.0 as terminal_id_risk_30min_window,
            0.0 as terminal_id_nb_tx_60min_window,
            0.0 as terminal_id_risk_60min_window
        FROM transaction_data d
        LEFT JOIN customer_features c ON c.customer_id = d.customer_id AND c.rn = 1
        LEFT JOIN terminal_features t ON t.terminal_id = d.terminal_id AND t.rn = 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("tx_amount", "FLOAT", float(transaction_data['TX_AMOUNT'])),
                bigquery.ScalarQueryParameter("terminal_id", "STRING", transaction_data['TERMINAL_ID']),
                bigquery.ScalarQueryParameter("customer_id", "STRING", transaction_data['CUSTOMER_ID']),
                bigquery.ScalarQueryParameter("tx_ts", "STRING", transaction_data['TX_TS'])
            ]
        )

        try:
            features_df = bq_client.query(features_query, job_config=job_config).to_dataframe()

            # Convert features to dictionary
            features = {}
            if not features_df.empty:
                features = features_df.iloc[0].to_dict()
                # Convert all values to float for consistency
                features = {k: float(v) if not isinstance(v, str) else v for k, v in features.items()}

            print(f"Successfully extracted features for TX_ID: {transaction_data['TX_ID']}")
            return features

        except Exception as e:
            print(f"Error extracting features: {str(e)}")
            print(f"Query parameters: {transaction_data}")
            raise

    def extract_prediction_probability(self, prediction_response):
        """
        Extract the fraud probability from the prediction response
        Args:
            prediction_response: Response from Vertex AI endpoint
        Returns:
            float: Probability of fraud
        """
        pred_value = prediction_response.predictions[0]

        print(f"Prediction response structure: {pred_value}")

        # BQML logistic regression model returns a specific format
        if isinstance(pred_value, dict):
            if 'tx_fraud_probs' in pred_value:
                # For BQML logistic regression models
                # Look at the tx_fraud_values to determine which probability corresponds to fraud
                fraud_index = pred_value['tx_fraud_values'].index('1')
                return float(pred_value['tx_fraud_probs'][fraud_index])

            # Handle other potential response formats
            elif 'scores' in pred_value:
                return float(pred_value['scores'][1])
            elif 'probability' in pred_value:
                return float(pred_value['probability'])

        # Default fallback options
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
        current_time = datetime.utcnow()

        # Ensure fraud_probability is a float
        fraud_probability = 0.0
        if prediction_data['fraud_probability'] is not None:
            fraud_probability = float(prediction_data['fraud_probability'])

        # Match the exact schema of the BigQuery table
        rows_to_insert = [{
            'TX_ID': prediction_data['TX_ID'],
            'fraud_probability': fraud_probability,
            'is_fraud': bool(fraud_probability > 0.5),
            'model_version': prediction_data['model_version'],
            'created_at': current_time.strftime('%Y-%m-%d %H:%M:%S UTC')
        }]

        try:
            errors = self.bq_client.insert_rows_json(self.table, rows_to_insert)
            if errors:
                print(f"Encountered errors while inserting rows: {errors}")
                print(f"Table schema: {[field.name for field in self.table.schema]}")
                print(f"Attempted to insert: {json.dumps(rows_to_insert, indent=2)}")
            else:
                print(f"Successfully inserted prediction for TX_ID: {prediction_data['TX_ID']}")
        except Exception as e:
            print(f"Error inserting into BigQuery: {str(e)}")
            print(f"Table schema: {[field.name for field in self.table.schema]}")
            print(f"Attempted to insert: {json.dumps(rows_to_insert, indent=2)}")
            raise

    def process_message(self, message):
        """
        Process a single Pub/Sub message
        Args:
            message: Pub/Sub message
        """
        try:
            message_data = json.loads(message.data.decode('utf-8'))
            print(f"Processing transaction {message_data['TX_ID']}")

            # Extract features with correct column names directly from the SQL query
            features = self.get_features(message_data)
            print(f"Features prepared: {json.dumps(features, indent=2)}")

            # Send features directly to the endpoint - no mapping needed
            prediction_response = self.endpoint.predict(instances=[features])
            print(f"Prediction response: {prediction_response.predictions[0]}")

            # Extract fraud probability
            fraud_probability = self.extract_prediction_probability(prediction_response)
            print(f"Extracted fraud probability: {fraud_probability}")

            # Prepare prediction result
            prediction_data = {
                'TX_ID': message_data['TX_ID'],
                'fraud_probability': fraud_probability,
                'model_version': prediction_response.deployed_model_id
            }
            print(f"Prediction result: {json.dumps(prediction_data, indent=2)}")

            # Save prediction
            self.save_prediction(prediction_data)

            # Acknowledge the message
            message.ack()
            print(f"Successfully processed transaction {message_data['TX_ID']}")

        except Exception as e:
            print(f"Error processing message: {str(e)}")
            print(f"Message data: {message.data.decode('utf-8')}")

            import traceback
            print("Traceback:", traceback.format_exc())

            # Negative acknowledge the message to retry
            message.nack()

    def start(self, subscription_path):
            """
            Start processing messages from Pub/Sub
            Args:
                subscription_path: Full path to the Pub/Sub subscription
            """
            subscriber = pubsub_v1.SubscriberClient()

            try:
                print(f"Listening for messages on {subscription_path}")
                streaming_pull_future = subscriber.subscribe(
                    subscription_path,
                    callback=self.process_message,
                    flow_control=pubsub_v1.types.FlowControl(max_messages=1)
                )

                # Keep the main thread alive
                streaming_pull_future.result()

            except KeyboardInterrupt:
                streaming_pull_future.cancel()
                subscriber.close()
            except Exception as e:
                print(f"Error in subscriber: {str(e)}")
                print("Traceback:", traceback.format_exc())
                subscriber.close()
                raise

def main():
    """Main function to run the processor"""
    processor = FraudDetectionProcessor(
        project_id=PROJECT_ID,
        endpoint_id=ENDPOINT_ID,
        location=LOCATION
    )

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