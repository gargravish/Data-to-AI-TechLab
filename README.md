# Data-to-AI-TechLab
## GCP Data to AI Tech Lab Challenge (fraudfinder)


## Step 0: Select your Google Cloud project
Please make sure that you have selected a Google Cloud project as shown below: image

## Step 1: Initial setup using Cloud Shell
Activate Cloud Shell in your project by clicking the Activate Cloud Shell button as shown in the image below. 

<image>

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
gcloud pubsub subscriptions create "ff-tx-for-feat-eng-sub" --topic="ff-tx" --topic-project="cymbal-fraudfinder"
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
## (C1) Step 3: EDA of transaction data in BigQuery

A.Transaction data summary statistics
B. Fraud Classification counts and percentages
C. Plot transaction amount distribution
D. Analyse customer-level aggregates of transaction data
E. Customer and Terminal Analysis

## (C2) Step 4: Feature Engineering

Focus on implementing three feature types (use Python or BQ for the same):
A. Transaction Amount Patterns
B. Time-based patterns
C. Merchant Risk Scoring

## (C3) Step 5: Model Development (BigQuery ML)

Focus on simple but effective model:
Use Random Forest Classifier
Implement cross-validation
Calculate key metrics (AUC-ROC, Precision, Recall)

## (C4) Step 6: Real-time Inference


