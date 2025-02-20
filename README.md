# Data-to-AI-TechLab
## GCP Data to AI Tech Lab Challenge (fraudfinder)


## Step 0: Select your Google Cloud project
Please make sure that you have selected a Google Cloud project as shown below: image

## Step 1: Initial setup using Cloud Shell
Activate Cloud Shell in your project by clicking the Activate Cloud Shell button as shown in the image below. 

![image](./images/activate-cloud-shell.png)

Once the Cloud Shell has activated, copy the following codes and execute them in the Cloud Shell to enable the necessary APIs, and create Pub/Sub subscriptions to read streaming transactions from public Pub/Sub topics.

Authorize the Cloud Shell if it prompts you to. Please note that this step may take a few minutes. You can navigate to the Pub/Sub console to verify the subscriptions.

```shell
gcloud services enable notebooks.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable aiplatform.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable dataflow.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable iam.googleapis.com

gcloud pubsub subscriptions create "ff-tx-sub" --topic="ff-tx" --topic-project="cymbal-fraudfinder"
gcloud pubsub subscriptions create "ff-txlabels-sub" --topic="ff-txlabels" --topic-project="cymbal-fraudfinder"

# Run the following command to grant the Compute Engine default service account access to read and write pipeline artifacts in Google Cloud Storage.
PROJECT_ID=$(gcloud config get-value project)
PROJECT_NUM=$(gcloud projects list --filter="$PROJECT_ID" --format="value(PROJECT_NUMBER)")
gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:${PROJECT_NUM}-compute@developer.gserviceaccount.com"\
      --role='roles/storage.admin'
gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:${PROJECT_NUM}@cloudbuild.gserviceaccount.com"\
      --role='roles/aiplatform.admin'
gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$PROJECT_NUM-compute@developer.gserviceaccount.com"\
      --role='roles/run.admin'
gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:$PROJECT_NUM-compute@developer.gserviceaccount.com"\
      --role='roles/resourcemanager.projectIamAdmin'
gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:service-${PROJECT_NUM}@gcp-sa-aiplatform.iam.gserviceaccount.com"\
      --role='roles/artifactregistry.writer'
gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member="serviceAccount:service-${PROJECT_NUM}@gcp-sa-aiplatform.iam.gserviceaccount.com"\
      --role='roles/storage.objectAdmin'   
```

## Step 2: Copy the historical transaction data into BigQuery tables
```shell
$ python3 scripts/copy_bigquery_data.py $BUCKET_NAME
```
## Step 3: Create BigQuery tables for realtime streaming data and Pub/Sub to BQ subscription
```sql
create table `{PROJECT_ID}.{Dataset_ID}.txlabels_realtime` as SELECT * FROM `{PROJECT_ID}.{Dataset_ID}.txlabels.txlabels` where 1=0;

create table `{PROJECT_ID}.{Dataset_ID}.tx_realtime` as SELECT * FROM `{PROJECT_ID}.{Dataset_ID}.tx` where 1=0;
```
## (C1) Step 4: EDA of transaction data in BigQuery

- Transaction data summary statistics
- Fraud Classification counts and percentages
- Plot transaction amount distribution
- Analyse customer-level aggregates of transaction data
- Customer and Terminal Analysis

## (C2) Step 5: Feature Engineering

Focus on implementing three feature types (use Python or BQ for the same):
- Transaction Amount Patterns
- Time-based patterns
- Merchant Risk Scoring
- Combine all features

## (C3) Step 6: Model Development (BigQuery ML)

Focus on simple but effective model:
- Use Random Forest Classifier
- Implement cross-validation
- Calculate key metrics (AUC-ROC, Precision, Recall)

![image](./images/fraud_finder_graph.png)

## (C4) Step 7: Model Inference
### Create BigQuery Table for online predictions
```sql
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{Dataset_ID}.online_fraud_prediction`
(
  TX_ID STRING,
  prediction_timestamp TIMESTAMP,
  fraud_probability FLOAT64,
  is_fraud BOOLEAN,
  model_version STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
### Deploy the model for online predictions
![image](./images/VertexAI_Model_Deploy.png)
- Get the VertexAI Endpoint ID
- Copy the "./C4-Real-Time Inference/fraud_online_inference.py" for online predictions to BigQuery Notebook and fill-in the required configurations.