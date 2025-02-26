--create ground truth table
--use past 15 days as the tran and predict period
CREATE OR REPLACE TABLE
  tx.ground_truth AS (
  SELECT
    raw_tx.TX_TS AS timestamp,
    raw_tx.CUSTOMER_ID AS customer,
    raw_tx.TERMINAL_ID AS terminal,
    raw_tx.TX_AMOUNT AS tx_amount,
    raw_lb.TX_FRAUD AS tx_fraud,
  FROM
    tx.tx AS raw_tx
  LEFT JOIN
    tx.txlabels AS raw_lb
  ON
    raw_tx.TX_ID = raw_lb.TX_ID
  WHERE
    DATE(raw_tx.TX_TS) > DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
    AND DATE(raw_tx.TX_TS) <= CURRENT_DATE());

--create train data by joining the ground truth table with all features
--use first 10 days as the tran period 
CREATE OR REPLACE TABLE
  tx.train_data AS (
  SELECT
    * EXCEPT (customer,
      terminal,
      feature_ts)
  FROM
    tx.ground_truth AS gt
  LEFT JOIN
    tx.customers_features AS customer
  ON
    gt.customer = customer.customer_id
    AND gt.timestamp = customer.feature_ts
  LEFT JOIN
    tx.terminals_features AS terminal
  ON
    gt.terminal = terminal.terminal_id
    AND gt.timestamp = terminal.feature_ts
  WHERE
    DATE(gt.timestamp) <= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
  AND tx_fraud IS NOT NULL);

--create predict data by joining the ground truth table with all features
--use last 5 days as the predict period 
CREATE OR REPLACE TABLE
  tx.predict_data AS (
  SELECT
    * EXCEPT (customer,
      terminal,
      feature_ts)
  FROM
    tx.ground_truth AS gt
  LEFT JOIN
    tx.customers_features AS customer
  ON
    gt.customer = customer.customer_id
    AND gt.timestamp = customer.feature_ts
  LEFT JOIN
    tx.terminals_features AS terminal
  ON
    gt.terminal = terminal.terminal_id
    AND gt.timestamp = terminal.feature_ts
  WHERE
    DATE(gt.timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
  AND tx_fraud IS NOT NULL);


--use logistic regression model
CREATE OR REPLACE MODEL
  tx.fraud_detection_logreg OPTIONS( MODEL_TYPE="LOGISTIC_REG",
    INPUT_LABEL_COLS=["tx_fraud"],
    EARLY_STOP=TRUE,
    MIN_REL_PROGRESS=0.01,
    model_registry="vertex_ai",
    vertex_ai_model_version_aliases=['logreg',
    'experimental'] ) AS
SELECT
  * EXCEPT(timestamp,
    customer_id,
    terminal_id)
FROM
  tx.train_data

--evaluate logreg model
SELECT
  *
FROM
  ML.EVALUATE (MODEL `tx.fraud_detection_logreg`);

--predict fraud using logreg model
SELECT
  *
FROM
  ML.PREDICT (MODEL `tx.fraud_detection_logreg`,
    (
    SELECT
      *
    FROM
      tx.predict_data));
    
--use xgboost model
CREATE OR REPLACE MODEL
  tx.fraud_detection_xgboost OPTIONS( model_type='BOOSTED_TREE_CLASSIFIER',
    input_label_cols=['tx_fraud'],
    num_parallel_tree=1,
    max_tree_depth=6,
    tree_method='hist',
    max_iterations=50,
    enable_global_explain=TRUE,
    learn_rate=0.1,
    early_stop=TRUE,
    l1_reg=0.1,
    l2_reg=0.1,
    subsample=0.8,
    colsample_bytree =0.8,
    model_registry="vertex_ai",
    vertex_ai_model_version_aliases=['xgboost',
    'experimental'] ) AS
SELECT
  * EXCEPT (timestamp,
    customer_id,
    terminal_id)
FROM
  tx.train_data;

--evaluate xgboost model
SELECT
  *
FROM
  ML.EVALUATE (MODEL `tx.fraud_detection_xgboost`);

--explain feature attributions
SELECT
  *
FROM
  ML.GLOBAL_EXPLAIN (MODEL `tx.fraud_detection_xgboost`);

--predict using xgboost model
SELECT
  *
FROM
  ML.PREDICT (MODEL `tx.fraud_detection_xgboost`,
    (
    SELECT
      *
    FROM
      tx.predict_data));

--create kmeans model with 8 clusters
CREATE OR REPLACE MODEL
  tx.fraud_detection_kmeans OPTIONS( MODEL_TYPE = 'kmeans',
    NUM_CLUSTERS = 8,
    KMEANS_INIT_METHOD = 'kmeans++' ) AS
SELECT
  * EXCEPT (timestamp,
    tx_fraud,
    customer_id,
    terminal_id)
FROM
  tx.train_data;

--evaluate kmeans model
SELECT
  davies_bouldin_index
FROM
  ML.EVALUATE(MODEL `tx.fraud_detection_kmeans`);

--display cluster characteristics
WITH
  T AS (
  SELECT
    centroid_id,
    ARRAY_AGG(STRUCT(feature AS name,
        ROUND(numerical_value,1) AS value)
    ORDER BY
      centroid_id) AS CLUSTER
  FROM
    ML.CENTROIDS(MODEL `tx.fraud_detection_kmeans`)
  GROUP BY
    centroid_id )
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


--detect anomalies/fraud using kmeans model
SELECT
  *
FROM
  ML.DETECT_ANOMALIES( MODEL `tx.fraud_detection_kmeans`,
    STRUCT(0.02 AS contamination),
    (
    SELECT
      *
    FROM
      tx.predict_data));