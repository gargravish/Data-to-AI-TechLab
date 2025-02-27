-- Fraud Detection ML Training & Prediction in BigQuery
-- Time to complete: ~1 hour 15 minutes
-- Follow the TODOs and hints to prepare train/predict data and create ML models

-- Part 1: Organise Training and Prediction Required Data Tables (? mins)

-- TODO: Create ground truth table
-- Hint: Pick today's date and go back 15 days as the time coverage
CREATE OR REPLACE TABLE
  tx.ground_truth AS (
  SELECT
    --select required fields
  FROM
    tx.tx AS raw_tx
  LEFT JOIN
    tx.txlabels AS raw_lb
  ON
    raw_tx.TX_ID = raw_lb.TX_ID
  WHERE
    --limit the date range
    );

-- TODO: Create training data table
-- Hint: Include all relevant features from the feature tables 
-- and use the first 10 days as the training period
CREATE OR REPLACE TABLE
  tx.train_data AS (
  SELECT
    --include relevant feature fields
  FROM
    tx.ground_truth AS gt
    --join with feature tables
  WHERE
    --limit date range
  AND tx_fraud IS NOT NULL);

-- TODO: Create prediction data table
-- Hint: Include all relevant features from the feature tables 
-- and use the last 5 days as the training period
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
-- Hint: Pick 'BOOSTED_TREE_CLASSIFIER' model type, use 'hist' tree method to improve training speed, register the model in Vertex AI    
--use xgboost model
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
-- TODO: List out the feature characteristics for each cluster centroid
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
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'tx_amount') AS tx_amount,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_nb_tx_1day_window') AS customer_id_nb_tx_1day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_nb_tx_7day_window') AS customer_id_nb_tx_7day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_nb_tx_14day_window') AS customer_id_nb_tx_14day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_avg_amount_1day_window') AS customer_id_avg_amount_1day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_avg_amount_7day_window') AS customer_id_avg_amount_7day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_avg_amount_14day_window') AS customer_id_avg_amount_14day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_nb_tx_15min_window') AS customer_id_nb_tx_15min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_nb_tx_30min_window') AS customer_id_nb_tx_30min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_nb_tx_60min_window') AS customer_id_nb_tx_60min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_avg_amount_15min_window') AS customer_id_avg_amount_15min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_avg_amount_30min_window') AS customer_id_avg_amount_30min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'customer_id_avg_amount_60min_window') AS customer_id_avg_amount_60min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_nb_tx_1day_window') AS terminal_id_nb_tx_1day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_nb_tx_7day_window') AS terminal_id_nb_tx_7day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_nb_tx_14day_window') AS terminal_id_nb_tx_14day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_risk_1day_window') AS terminal_id_risk_1day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_risk_7day_window') AS terminal_id_risk_7day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_risk_14day_window') AS terminal_id_risk_14day_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_nb_tx_15min_window') AS terminal_id_nb_tx_15min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_nb_tx_30min_window') AS terminal_id_nb_tx_30min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_nb_tx_60min_window') AS terminal_id_nb_tx_60min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_risk_15min_window') AS terminal_id_avg_amount_15min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_risk_30min_window') AS terminal_id_avg_amount_30min_window,
  (
  SELECT
    value
  FROM
    UNNEST(CLUSTER)
  WHERE
    name = 'terminal_id_risk_60min_window') AS terminal_id_avg_amount_60min_window
FROM
  T
ORDER BY
  centroid_id ASC;


-- TODO: Run anomaly/fraud detection based on the clustering model created above
SELECT
  *
FROM
  --call detect anomaly function;