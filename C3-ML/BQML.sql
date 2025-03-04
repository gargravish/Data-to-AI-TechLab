-- Fraud Detection ML Training & Prediction in BigQuery
-- Time to complete: ~1 hour 15 minutes
-- Follow the TODOs and hints to prepare train/predict data and create ML models

-- Part 1: Organise Training and Prediction Required Data Tables (? mins)

-- TODO: Create the training data table, covering the first 10 days of the last 15 days from the current timestamp
-- Hint: Use TIMESTAMP_SUB() and CURRENT_TIMESTAMP() functions
CREATE OR REPLACE TABLE
  tx.train_data AS (
  WITH ground_truth AS (
  SELECT
    --select required fields
  FROM
    tx.tx AS t
  LEFT JOIN
    tx.txlabels AS l
  ON
    t.TX_ID = l.TX_ID
    )
  SELECT
    --include relevant feature fields
  FROM
    ground_truth AS gt
  LEFT JOIN
    --join with feature tables
  WHERE
    --limit the date range  gt.timestamp BETWEEN ... TIMESTAMP_SUB(...) AND ... 
  AND tx_fraud IS NOT NULL);


-- TODO: Create the testing data table, covering the remaning 5 days of the last 15 days from the current timestamp
-- Hint: Use TIMESTAMP_SUB() and CURRENT_TIMESTAMP() functions
CREATE OR REPLACE TABLE
  tx.predict_data AS (
    --complete sql script
);

-- Part 2: Carry Out Model Training, Evaluation and Prediction through Three Different Model Approaches (? mins)

-- TODO: Create a logistic regression model
-- Hint: Pick 'LOGISTIC_REG' model type, register the model in Vertex AI
CREATE OR REPLACE MODEL
  tx.fraud_detection_logreg OPTIONS(
    --specify model options
  ) AS
SELECT
  * EXCEPT(timestamp,
    customer_id,
    terminal_id)
FROM
  tx.train_data

-- TODO: Evaluate the model created above
SELECT
  *
FROM
  --call evaluate function;

-- TODO: Run predictions using the model created above
SELECT
  *
FROM
  --call predict function;
    
-- TODO: Create a xgboost model
-- Hint: Pick 'BOOSTED_TREE_CLASSIFIER' model type, use 'hist' tree method to improve training speed, use 'class_weights' to handle imbalance, register the model in Vertex AI
CREATE OR REPLACE MODEL
  tx.fraud_detection_xgboost OPTIONS(
    --specify model options
  ) AS
SELECT
  * EXCEPT (timestamp,
    customer_id,
    terminal_id)
FROM
  tx.train_data;

-- TODO: Evaluate the model created above
SELECT
  *
FROM
  --call evaluate function;


-- TODO: Explain model feature attributions
SELECT
  *
FROM
  --call explain function;

-- TODO: Run predictions using the model created above
SELECT
  *
FROM
  --call predict function);


-- TODO: Create a kmeans clutering model
-- Hint: Use 'kmeans' as the model type, decide how many clusters you will allocate
CREATE OR REPLACE MODEL
  tx.fraud_detection_kmeans OPTIONS(
    --specify model options
  ) AS
SELECT
  * EXCEPT (timestamp,
    tx_fraud,
    customer_id,
    terminal_id)
FROM
  tx.train_data;

-- TODO: Evaluate the model created above
SELECT
  davies_bouldin_index
FROM
  --call evaluate function;

--[Optional]
-- TODO: List out the feature characteristics for each cluster centroid and pivot them to separate columns
-- Hint: Use ARRAY_AGG() and STRUCT() functions
WITH
  T AS (
  SELECT
    centroid_id,
    --create an array of structs containing the feature name and rounded numerical value.
    ORDER BY
      centroid_id) AS CLUSTER
  FROM
    ML.CENTROIDS(MODEL `your_model_name`)
  GROUP BY
    centroid_id)
SELECT
  CONCAT('Cluster#', CAST(centroid_id AS STRING)) AS centroid,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'tx_amount') AS tx_amount,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_nb_tx_1day_window') AS customer_id_nb_tx_1day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_nb_tx_7day_window') AS customer_id_nb_tx_7day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_nb_tx_14day_window') AS customer_id_nb_tx_14day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_avg_amount_1day_window') AS customer_id_avg_amount_1day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_avg_amount_7day_window') AS customer_id_avg_amount_7day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_avg_amount_14day_window') AS customer_id_avg_amount_14day_window,
  (SELECT value  FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_nb_tx_15min_window') AS customer_id_nb_tx_15min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_nb_tx_30min_window') AS customer_id_nb_tx_30min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_nb_tx_60min_window') AS customer_id_nb_tx_60min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_avg_amount_15min_window') AS customer_id_avg_amount_15min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_avg_amount_30min_window') AS customer_id_avg_amount_30min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'customer_id_avg_amount_60min_window') AS customer_id_avg_amount_60min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_nb_tx_1day_window') AS terminal_id_nb_tx_1day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_nb_tx_7day_window') AS terminal_id_nb_tx_7day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_nb_tx_14day_window') AS terminal_id_nb_tx_14day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_risk_1day_window') AS terminal_id_risk_1day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_risk_7day_window') AS terminal_id_risk_7day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_risk_14day_window') AS terminal_id_risk_14day_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_nb_tx_15min_window') AS terminal_id_nb_tx_15min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_nb_tx_30min_window') AS terminal_id_nb_tx_30min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_nb_tx_60min_window') AS terminal_id_nb_tx_60min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_risk_15min_window') AS terminal_id_avg_amount_15min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_risk_30min_window') AS terminal_id_avg_amount_30min_window,
  (SELECT value FROM UNNEST(CLUSTER)
  WHERE name = 'terminal_id_risk_60min_window') AS terminal_id_avg_amount_60min_window
FROM
  T
ORDER BY
  centroid_id ASC;


-- TODO: Run anomaly/fraud detection based on the clustering model created above
SELECT
  *
FROM
  --call detect anomaly function;