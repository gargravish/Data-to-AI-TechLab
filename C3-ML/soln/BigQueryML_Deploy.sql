-- Ground Truth Table Creation
CREATE OR REPLACE TABLE tx.ground_truth AS (
  SELECT
    t.TX_TS AS timestamp,
    t.CUSTOMER_ID AS customer_id,
    t.TERMINAL_ID AS terminal_id,
    t.TX_AMOUNT AS tx_amount,
    l.TX_FRAUD AS tx_fraud
  FROM
    tx.tx AS t
  LEFT JOIN
    tx.txlabels AS l
  ON
    t.TX_ID = l.TX_ID
  WHERE
    DATE(t.TX_TS) > DATE_SUB("2025-08-28", INTERVAL 15 DAY)
    AND DATE(t.TX_TS) <= "2025-08-28"
);

-- Training Data Creation
CREATE OR REPLACE TABLE tx.train_data AS (
  SELECT
    gt.timestamp,
    gt.tx_amount,
    
    -- Customer features
    cf.customer_id_nb_tx_15min_window,
    cf.customer_id_avg_amount_15min_window,
    cf.customer_id_nb_tx_30min_window,
    cf.customer_id_avg_amount_30min_window,
    cf.customer_id_nb_tx_60min_window,
    cf.customer_id_avg_amount_60min_window,
    cf.customer_id_nb_tx_1day_window,
    cf.customer_id_avg_amount_1day_window,
    cf.customer_id_nb_tx_7day_window,
    cf.customer_id_avg_amount_7day_window,
    cf.customer_id_nb_tx_14day_window,
    cf.customer_id_avg_amount_14day_window,
    
    -- Terminal features
    gt.terminal_id,
    tf.terminal_id_nb_tx_15min_window,
    tf.terminal_id_risk_15min_window,
    tf.terminal_id_nb_tx_30min_window,
    tf.terminal_id_risk_30min_window,
    tf.terminal_id_nb_tx_60min_window,
    tf.terminal_id_risk_60min_window,
    tf.terminal_id_nb_tx_1day_window,
    tf.terminal_id_risk_1day_window,
    tf.terminal_id_nb_tx_7day_window,
    tf.terminal_id_risk_7day_window,
    tf.terminal_id_nb_tx_14day_window,
    tf.terminal_id_risk_14day_window,
    
    -- Label
    gt.tx_fraud
  FROM
    tx.ground_truth AS gt
  LEFT JOIN
    tx.customer_spending_features AS cf
  ON
    gt.customer_id = cf.customer_id
    AND gt.timestamp = cf.feature_ts
  LEFT JOIN
    tx.terminal_risk_features AS tf
  ON
    gt.terminal_id = tf.terminal_id
    AND gt.timestamp = tf.feature_ts
  WHERE
    DATE(gt.timestamp) <= DATE_SUB("2025-08-28", INTERVAL 5 DAY)
    AND gt.tx_fraud IS NOT NULL
);

-- Prediction Data Creation
CREATE OR REPLACE TABLE tx.predict_data AS (
  SELECT
    gt.timestamp,
    gt.tx_amount,
    
    -- Customer features
    cf.customer_id_nb_tx_15min_window,
    cf.customer_id_avg_amount_15min_window,
    cf.customer_id_nb_tx_30min_window,
    cf.customer_id_avg_amount_30min_window,
    cf.customer_id_nb_tx_60min_window,
    cf.customer_id_avg_amount_60min_window,
    cf.customer_id_nb_tx_1day_window,
    cf.customer_id_avg_amount_1day_window,
    cf.customer_id_nb_tx_7day_window,
    cf.customer_id_avg_amount_7day_window,
    cf.customer_id_nb_tx_14day_window,
    cf.customer_id_avg_amount_14day_window,
    
    -- Terminal features
    gt.terminal_id,
    tf.terminal_id_nb_tx_15min_window,
    tf.terminal_id_risk_15min_window,
    tf.terminal_id_nb_tx_30min_window,
    tf.terminal_id_risk_30min_window,
    tf.terminal_id_nb_tx_60min_window,
    tf.terminal_id_risk_60min_window,
    tf.terminal_id_nb_tx_1day_window,
    tf.terminal_id_risk_1day_window,
    tf.terminal_id_nb_tx_7day_window,
    tf.terminal_id_risk_7day_window,
    tf.terminal_id_nb_tx_14day_window,
    tf.terminal_id_risk_14day_window,
    
    -- Label
    gt.tx_fraud
  FROM
    tx.ground_truth AS gt
  LEFT JOIN
    tx.customer_spending_features AS cf
  ON
    gt.customer_id = cf.customer_id
    AND gt.timestamp = cf.feature_ts
  LEFT JOIN
    tx.terminal_risk_features AS tf
  ON
    gt.terminal_id = tf.terminal_id
    AND gt.timestamp = tf.feature_ts
  WHERE
    DATE(gt.timestamp) > DATE_SUB("2025-08-28", INTERVAL 5 DAY)
    AND gt.tx_fraud IS NOT NULL
);

-- LogReg BQML Model
CREATE OR REPLACE MODEL tx.fraud_detection_logreg
OPTIONS(
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['tx_fraud'],
  early_stop = TRUE,
  min_rel_progress = 0.01,
  model_registry = "VERTEX_AI",
  vertex_ai_model_version_aliases = ['logistic_reg', 'fraud_models'],
  enable_global_explain = TRUE
) AS
SELECT
  * EXCEPT(timestamp, terminal_id)
FROM
  tx.train_data;

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