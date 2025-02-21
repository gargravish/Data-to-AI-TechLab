DECLARE YESTERDAY DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
DECLARE DATAPROCESSING_END_DATE DATE DEFAULT YESTERDAY;

CREATE OR REPLACE TABLE `PROJECT_ID.DATASET_ID.TABLE_ID` AS --- UPDATE RELEVANT TABLE_ID
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
      `PROJECT_ID.TX.TX` --- UPDATE PROJECT_ID
    WHERE
      DATE(TX_TS) BETWEEN DATE_SUB(DATAPROCESSING_END_DATE, INTERVAL 15 DAY) AND DATAPROCESSING_END_DATE
    ) raw_tx
  LEFT JOIN 
    `PROJECT_ID.tx.txlabels` as raw_lb --- UPDATE PROJECT_ID
  ON raw_tx.TX_ID = raw_lb.TX_ID),

  # query to calculate TERMINAL spending behaviour --------------------------------------------------------------------------------
  get_variables_delay_window AS (
  SELECT
    TX_TS,
    TX_ID,
    CUSTOMER_ID,
    TERMINAL_ID,
    
    # calc total amount of fraudulent tx and the total number of tx over the delay period per terminal (7 days - delay, expressed in seconds)
    SUM(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 604800 PRECEDING
      AND CURRENT ROW ) AS NB_FRAUD_DELAY,
    COUNT(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 604800 PRECEDING
      AND CURRENT ROW ) AS NB_TX_DELAY,
      
    # calc total amount of fraudulent tx and the total number of tx over the delayed window per terminal (window + 7 days - delay, expressed in seconds)
    SUM(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 691200 PRECEDING
      AND CURRENT ROW ) AS NB_FRAUD_1_DELAY_WINDOW,
    SUM(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 1209600 PRECEDING
      AND CURRENT ROW ) AS NB_FRAUD_7_DELAY_WINDOW,
    SUM(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 1814400 PRECEDING
      AND CURRENT ROW ) AS NB_FRAUD_14_DELAY_WINDOW,
    COUNT(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 691200 PRECEDING
      AND CURRENT ROW ) AS NB_TX_1_DELAY_WINDOW,
    COUNT(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 1209600 PRECEDING
      AND CURRENT ROW ) AS NB_TX_7_DELAY_WINDOW,
    COUNT(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 1814400 PRECEDING
      AND CURRENT ROW ) AS NB_TX_14_DELAY_WINDOW,
  FROM get_raw_table),

  # query to calculate TERMINAL risk factors ---------------------------------------------------------------------------------------
  get_risk_factors AS (
  SELECT
    TX_TS,
    TX_ID,
    CUSTOMER_ID,
    TERMINAL_ID,
    # calculate numerator of risk index
    NB_FRAUD_1_DELAY_WINDOW - NB_FRAUD_DELAY AS TERMINAL_ID_NB_FRAUD_1DAY_WINDOW,
    NB_FRAUD_7_DELAY_WINDOW - NB_FRAUD_DELAY AS TERMINAL_ID_NB_FRAUD_7DAY_WINDOW,
    NB_FRAUD_14_DELAY_WINDOW - NB_FRAUD_DELAY AS TERMINAL_ID_NB_FRAUD_14DAY_WINDOW,
    # calculate denominator of risk index
    NB_TX_1_DELAY_WINDOW - NB_TX_DELAY AS TERMINAL_ID_NB_TX_1DAY_WINDOW,
    NB_TX_7_DELAY_WINDOW - NB_TX_DELAY AS TERMINAL_ID_NB_TX_7DAY_WINDOW,
    NB_TX_14_DELAY_WINDOW - NB_TX_DELAY AS TERMINAL_ID_NB_TX_14DAY_WINDOW,
      FROM
    get_variables_delay_window),

  # query to calculate the TERMINAL risk index -------------------------------------------------------------------------------------
  get_risk_index AS (
    SELECT
    TX_TS,
    TX_ID,
    CUSTOMER_ID,
    TERMINAL_ID,
    TERMINAL_ID_NB_TX_1DAY_WINDOW,
    TERMINAL_ID_NB_TX_7DAY_WINDOW,
    TERMINAL_ID_NB_TX_14DAY_WINDOW,
    # calculate the risk index
    (TERMINAL_ID_NB_FRAUD_1DAY_WINDOW/(TERMINAL_ID_NB_TX_1DAY_WINDOW+0.0001)) AS TERMINAL_ID_RISK_1DAY_WINDOW,
    (TERMINAL_ID_NB_FRAUD_7DAY_WINDOW/(TERMINAL_ID_NB_TX_7DAY_WINDOW+0.0001)) AS TERMINAL_ID_RISK_7DAY_WINDOW,
    (TERMINAL_ID_NB_FRAUD_14DAY_WINDOW/(TERMINAL_ID_NB_TX_14DAY_WINDOW+0.0001)) AS TERMINAL_ID_RISK_14DAY_WINDOW
    FROM get_risk_factors 
  )

# Create the table with CUSTOMER and TERMINAL features ----------------------------------------------------------------------------
SELECT
  PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TX_TS, "UTC")) AS feature_ts,
  TERMINAL_ID AS terminal_id,
  CAST(TERMINAL_ID_NB_TX_1DAY_WINDOW AS INT64) AS terminal_id_nb_tx_1day_window,
  CAST(TERMINAL_ID_NB_TX_7DAY_WINDOW AS INT64) AS terminal_id_nb_tx_7day_window,
  CAST(TERMINAL_ID_NB_TX_14DAY_WINDOW AS INT64) AS terminal_id_nb_tx_14day_window,
  CAST(TERMINAL_ID_RISK_1DAY_WINDOW AS FLOAT64) AS terminal_id_risk_1day_window,
  CAST(TERMINAL_ID_RISK_7DAY_WINDOW AS FLOAT64) AS terminal_id_risk_7day_window,
  CAST(TERMINAL_ID_RISK_14DAY_WINDOW AS FLOAT64) AS terminal_id_risk_14day_window,
FROM
  get_risk_index
;