-- Customer Spending Features {FraudFinder}

CREATE OR REPLACE view tx.customer_spending_features AS
WITH
  -- query to join labels with features -------------------------------------------------------------------------------------------
  get_raw_table AS (
  SELECT
    raw_tx.TX_TS,
    raw_tx.TX_ID,
    raw_tx.CUSTOMER_ID,
    raw_tx.TERMINAL_ID,
    raw_tx.TX_AMOUNT,
    raw_lb.TX_FRAUD
  FROM (
    SELECT
      *
    FROM
      tx.tx
    WHERE
      TX_TS BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 DAY) AND CURRENT_TIMESTAMP()
    ) raw_tx
  LEFT JOIN 
    tx.txlabels as raw_lb
  ON raw_tx.TX_ID = raw_lb.TX_ID),

  -- query to calculate CUSTOMER spending behaviour --------------------------------------------------------------------------------
  get_customer_spending_behaviour AS (
  SELECT
    TX_TS,
    TX_ID,
    CUSTOMER_ID,
    TERMINAL_ID,
    TX_AMOUNT,
    TX_FRAUD,
    
    # calc the number of customer tx over minute windows per customer (15, 30 and 60 minutes, expressed in seconds)
    COUNT(TX_FRAUD) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 900 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_NB_TX_15MIN_WINDOW,
    COUNT(TX_FRAUD) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 1800 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_NB_TX_30MIN_WINDOW,
    COUNT(TX_FRAUD) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 3600 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_NB_TX_60MIN_WINDOW,
    
    # calc the number of customer tx over daily windows per customer (1, 7 and 14 days, expressed in seconds)
    COUNT(TX_FRAUD) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 86400 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_NB_TX_1DAY_WINDOW,
    COUNT(TX_FRAUD) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 604800 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_NB_TX_7DAY_WINDOW,
    COUNT(TX_FRAUD) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 1209600 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_NB_TX_14DAY_WINDOW,
      
    # calc the customer average tx amount over minute windows per customer (15, 30 and 60 minutes, expressed in seconds, in dollars ($))
    AVG(TX_AMOUNT) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 900 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_AVG_AMOUNT_15MIN_WINDOW,
    AVG(TX_AMOUNT) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 1800 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_AVG_AMOUNT_30MIN_WINDOW,
    AVG(TX_AMOUNT) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 3600 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_AVG_AMOUNT_60MIN_WINDOW,
      
    # calc the customer average tx amount over daily windows per customer (1, 7 and 14 days, expressed in seconds, in dollars ($))
    AVG(TX_AMOUNT) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 86400 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW,
    AVG(TX_AMOUNT) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 604800 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW,
    AVG(TX_AMOUNT) OVER (PARTITION BY CUSTOMER_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 1209600 PRECEDING
      AND CURRENT ROW ) AS CUSTOMER_ID_AVG_AMOUNT_14DAY_WINDOW
  FROM get_raw_table)

# Create the table with CUSTOMER and TERMINAL features ----------------------------------------------------------------------------
SELECT
  PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TX_TS, "UTC")) AS feature_ts,
  CUSTOMER_ID AS customer_id,
  CAST(CUSTOMER_ID_NB_TX_15MIN_WINDOW AS INT64) AS customer_id_nb_tx_15min_window,
  CAST(CUSTOMER_ID_NB_TX_30MIN_WINDOW AS INT64) AS customer_id_nb_tx_30min_window,
  CAST(CUSTOMER_ID_NB_TX_60MIN_WINDOW AS INT64) AS customer_id_nb_tx_60min_window,
  CAST(CUSTOMER_ID_NB_TX_1DAY_WINDOW AS INT64) AS customer_id_nb_tx_1day_window,
  CAST(CUSTOMER_ID_NB_TX_7DAY_WINDOW AS INT64) AS customer_id_nb_tx_7day_window,
  CAST(CUSTOMER_ID_NB_TX_14DAY_WINDOW AS INT64) AS customer_id_nb_tx_14day_window,
  CAST(CUSTOMER_ID_AVG_AMOUNT_15MIN_WINDOW AS FLOAT64) AS customer_id_avg_amount_15min_window,
  CAST(CUSTOMER_ID_AVG_AMOUNT_30MIN_WINDOW AS FLOAT64) AS customer_id_avg_amount_30min_window,
  CAST(CUSTOMER_ID_AVG_AMOUNT_60MIN_WINDOW AS FLOAT64) AS customer_id_avg_amount_60min_window,
  CAST(CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW AS FLOAT64) AS customer_id_avg_amount_1day_window,
  CAST(CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW AS FLOAT64) AS customer_id_avg_amount_7day_window,
  CAST(CUSTOMER_ID_AVG_AMOUNT_14DAY_WINDOW AS FLOAT64) AS customer_id_avg_amount_14day_window
FROM
  get_customer_spending_behaviour;
