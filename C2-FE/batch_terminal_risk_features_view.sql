-- Terminal Features (batch)
-- Time to complete: 35 minutes
-- Follow the TODOs and HINTS to compute terminal risk features from raw data

CREATE OR REPLACE view tx.terminal_risk_features AS
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
    -- TODO: Add a subquery here to select data from the tx.tx table for the last 15 days window from current timestamp
    -- HINT: Use the TIMESTAMP_SUB() and CURRENT_TIMESTAMP() functions
    -- Example: SELECT * FROM tx.tx WHERE TX_TS BETWEEN ... TIMESTAMP_SUB(...) AND ...
    ) raw_tx
  LEFT JOIN 
    tx.txlabels as raw_lb
  ON raw_tx.TX_ID = raw_lb.TX_ID),

  # query to calculate TERMINAL spending behaviour --------------------------------------------------------------------------------
  get_variables_delay_window AS (
  SELECT
    TX_TS,
    TX_ID,
    CUSTOMER_ID,
    TERMINAL_ID,

    # calc total amount of fraudulent tx over the delay period per terminal (7 days - delay, expressed in seconds)
    -- here is the 7 day fraud delay period --
    SUM(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 604800 PRECEDING
      AND CURRENT ROW ) AS NB_FRAUD_DELAY,

    # calc total amount of fraudulent tx over the delayed window per terminal (window + 7 days - delay, expressed in seconds)
    -- TODO: Follow the given example for 15 min over the delayed 7-day window to complete for 30 min, 60 min, 1 day, 7 day and 14 day windows
    -- HINT: The min and day window delays are added to the 7-day fraud delay e.g. 15 min delay window is 604800 + 900 = 605700 seconds
    SUM(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 605700 PRECEDING
      AND CURRENT ROW ) AS NB_FRAUD_15MIN_DELAY_WINDOW,
    -- Add query for 30 min delay window (use alias NB_FRAUD_30MIN_DELAY_WINDOW) --
    -- Add query for 60 min delay window (use alias NB_FRAUD_60MIN_DELAY_WINDOW) --
    -- Add query for 1 day delay window (use alias NB_FRAUD_1_DELAY_WINDOW) --
    -- Add query for 7 day delay window (use alias NB_FRAUD_7_DELAY_WINDOW) --
    -- Add query for 14 day delay window (use alias NB_FRAUD_14_DELAY_WINDOW) --

    # calc total number of tx over the delay period per terminal (7 days - delay, expressed in seconds)
    -- here is the 7 day tx delay period --
    COUNT(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 604800 PRECEDING
      AND CURRENT ROW ) AS NB_TX_DELAY,

    # calc the total number of tx over the delayed window per terminal (window + 7 days - delay, expressed in seconds)
    -- TODO: Follow the given example for 15 min over the delayed 7-day window to complete for 30 min, 60 min, 1 day, 7 day and 14 day windows
    -- HINT: The min and day window delays are added to the 7-day tx delay e.g. 15 min delay window is 604800 + 900 = 605700 seconds
    COUNT(TX_FRAUD) OVER (PARTITION BY TERMINAL_ID ORDER BY UNIX_SECONDS(TX_TS) ASC RANGE BETWEEN 605700 PRECEDING
      AND CURRENT ROW ) AS NB_TX_15MIN_DELAY_WINDOW,
    -- Add query for 30 min delay window (use alias NB_TX_30MIN_DELAY_WINDOW) --
    -- Add query for 60 min delay window (use alias NB_TX_60MIN_DELAY_WINDOW) --
    -- Add query for 1 day delay window (use alias NB_TX_1_DELAY_WINDOW) --
    -- Add query for 7 day delay window (use alias NB_TX_7_DELAY_WINDOW) --
    -- Add query for 14 day delay window (use alias NB_TX_14_DELAY_WINDOW) --
  FROM get_raw_table),

  # query to calculate TERMINAL risk factors ---------------------------------------------------------------------------------------
  get_risk_factors AS (
  SELECT
    TX_TS,
    TX_ID,
    CUSTOMER_ID,
    TERMINAL_ID,
    # calculate numerator of risk index
    -- TODO: Follow the given 15 min example to complete for 30 min, 60 min, 1 day, 7 day and 14 day
    NB_FRAUD_15MIN_DELAY_WINDOW - NB_FRAUD_DELAY AS TERMINAL_ID_NB_FRAUD_15MIN_WINDOW,
    -- Add query for 30 min (use alias TERMINAL_ID_NB_FRAUD_30MIN_WINDOW) --
    -- Add query for 60 min (use alias TERMINAL_ID_NB_FRAUD_60MIN_WINDOW) --
    -- Add query for 1 day (use alias TERMINAL_ID_NB_FRAUD_1DAY_WINDOW) --
    -- Add query for 7 day (use alias TERMINAL_ID_NB_FRAUD_7DAY_WINDOW) --
    -- Add query for 14 day (use alias TERMINAL_ID_NB_FRAUD_14DAY_WINDOW) --    
    # calculate denominator of risk index
    -- TODO: Follow the given 15 min example to complete for 30 min, 60 min, 1 day, 7 day and 14 day
    NB_TX_15MIN_DELAY_WINDOW - NB_TX_DELAY AS TERMINAL_ID_NB_TX_15MIN_WINDOW,
    -- Add query for 30 min (use alias TERMINAL_ID_NB_TX_30MIN_WINDOW) --
    -- Add query for 60 min (use alias TERMINAL_ID_NB_TX_60MIN_WINDOW) --
    -- Add query for 1 day (use alias TERMINAL_ID_NB_TX_1DAY_WINDOW) --
    -- Add query for 7 day (use alias TERMINAL_ID_NB_TX_7DAY_WINDOW) --
    -- Add query for 14 day (use alias TERMINAL_ID_NB_TX_14DAY_WINDOW) --  
  FROM get_variables_delay_window),

  # query to calculate the TERMINAL risk index -------------------------------------------------------------------------------------
  get_risk_index AS (
    SELECT
    TX_TS,
    TX_ID,
    CUSTOMER_ID,
    TERMINAL_ID,
    TERMINAL_ID_NB_TX_15MIN_WINDOW,
    TERMINAL_ID_NB_TX_30MIN_WINDOW,
    TERMINAL_ID_NB_TX_60MIN_WINDOW,
    TERMINAL_ID_NB_TX_1DAY_WINDOW,
    TERMINAL_ID_NB_TX_7DAY_WINDOW,
    TERMINAL_ID_NB_TX_14DAY_WINDOW,
    # calculate the risk index
    -- TODO: Follow the given 15 min example to compute terminal risk for 30 min, 60 min, 1 day, 7 day and 14 day
    -- NOTE: We add a 0.0001 to prevent a division by 0 
    (TERMINAL_ID_NB_FRAUD_15MIN_WINDOW/(TERMINAL_ID_NB_TX_15MIN_WINDOW+0.0001)) AS TERMINAL_ID_RISK_15MIN_WINDOW,
    -- Add query for 30 min (use alias TERMINAL_ID_RISK_30MIN_WINDOW) --
    -- Add query for 60 min (use alias TERMINAL_ID_RISK_60MIN_WINDOW) --
    -- Add query for 1 day (use alias TERMINAL_ID_RISK_1DAY_WINDOW) --
    -- Add query for 7 day (use alias TERMINAL_ID_RISK_7DAY_WINDOW) --
    -- Add query for 14 day (use alias TERMINAL_ID_RISK_14DAY_WINDOW) --
    FROM get_risk_factors 
  )

-- query to create the TERMINAL RISK features ----------------------------------------------------------------------------
SELECT
  PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TX_TS, "UTC")) AS feature_ts,
  TERMINAL_ID AS terminal_id,
  CAST(TERMINAL_ID_NB_TX_15MIN_WINDOW AS INT64) AS terminal_id_nb_tx_15min_window,
  CAST(TERMINAL_ID_NB_TX_30MIN_WINDOW AS INT64) AS terminal_id_nb_tx_30min_window,
  CAST(TERMINAL_ID_NB_TX_60MIN_WINDOW AS INT64) AS terminal_id_nb_tx_60min_window,
  CAST(TERMINAL_ID_NB_TX_1DAY_WINDOW AS INT64) AS terminal_id_nb_tx_1day_window,
  CAST(TERMINAL_ID_NB_TX_7DAY_WINDOW AS INT64) AS terminal_id_nb_tx_7day_window,
  CAST(TERMINAL_ID_NB_TX_14DAY_WINDOW AS INT64) AS terminal_id_nb_tx_14day_window,
  CAST(TERMINAL_ID_RISK_15MIN_WINDOW AS FLOAT64) AS terminal_id_risk_15min_window,
  CAST(TERMINAL_ID_RISK_30MIN_WINDOW AS FLOAT64) AS terminal_id_risk_30min_window,
  CAST(TERMINAL_ID_RISK_60MIN_WINDOW AS FLOAT64) AS terminal_id_risk_60min_window,
  CAST(TERMINAL_ID_RISK_1DAY_WINDOW AS FLOAT64) AS terminal_id_risk_1day_window,
  CAST(TERMINAL_ID_RISK_7DAY_WINDOW AS FLOAT64) AS terminal_id_risk_7day_window,
  CAST(TERMINAL_ID_RISK_14DAY_WINDOW AS FLOAT64) AS terminal_id_risk_14day_window
FROM
  get_risk_index;