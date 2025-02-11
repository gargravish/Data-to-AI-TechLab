-- Fraud Detection EDA in BigQuery
-- Time to complete: ~1 hour
-- Follow the TODOs and hints to explore the transaction data

-- Part 1: Basic Transaction Analysis (15 mins)

-- TODO: Calculate basic transaction statistics
-- Hint: Use aggregate functions to understand your data volume and range
SELECT
  ... AS NUM_TX,
  MIN(TX_TS) AS MIN_TX_DATE,
  ... AS MAX_TX_DATE,
  COUNT(DISTINCT CUSTOMER_ID) AS NUM_CUSTOMERS,
  ... AS NUM_TERMINALS,
  MIN(TX_AMOUNT) AS MIN_TX_AMOUNT,
  ... AS AVG_TX_AMOUNT,
  ... AS MAX_TX_AMOUNT
FROM
  tx.tx;

-- TODO: Analyze fraud distribution
-- Hint: Calculate both counts and percentages of fraudulent vs non-fraudulent transactions
SELECT
  TX_FRAUD,
  COUNT(*) AS NUM_TX,
  SUM(COUNT(*)) OVER () AS OVR_TOTAL_TX,
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER ()
    ) AS PCT_TOTAL_TX
FROM
  tx.txlabels
GROUP BY
  TX_FRAUD
ORDER BY
  TX_FRAUD;

-- Part 2: Amount Distribution Analysis (15 mins)

-- TODO: Create transaction amount distribution
-- Hint: Round amounts to analyze common transaction values
SELECT
  ROUND(TX_AMOUNT, 0) AS ROUNDED_TX_AMOUNT,
  COUNT(*) AS NUM_TX
FROM
  tx.tx
GROUP BY
  ROUNDED_TX_AMOUNT
ORDER BY
  ROUNDED_TX_AMOUNT;

-- TODO: Analyze amount patterns by fraud status
-- Hint: Join transaction data with fraud labels
SELECT
  TX_FRAUD,
  MIN(TX_AMOUNT) AS MIN_TX_AMOUNT,
  AVG(TX_AMOUNT) AS AVG_TX_AMOUNT,
  MAX(TX_AMOUNT) AS MAX_TX_AMOUNT,
  COUNT(*) AS NUM_TX
FROM
  tx.tx
JOIN
  tx.txlabels USING (TX_ID)
GROUP BY
  TX_FRAUD;

-- Part 3: Customer Analysis (15 mins)

-- TODO: Calculate customer-level metrics
-- Hint: Consider transaction counts, amounts, and fraud rates
SELECT
  CUSTOMER_ID,
  ... AS NUM_TX,
  ... AS AVG_TX_AMOUNT,
  ... AS PCT_TX_FRAUD
FROM
  tx.tx
LEFT JOIN
  tx.txlabels USING (TX_ID)
GROUP BY
  CUSTOMER_ID;

-- TODO: Identify high-risk customers
-- Hint: Look for unusual patterns in transaction behavior
SELECT
  CUSTOMER_ID,
  ... as num_transactions,
  ... AS PCT_TX_FRAUD,
  AVG(TX_AMOUNT) AS AVG_TX_AMOUNT
FROM
  tx.tx
LEFT JOIN
  tx.txlabels USING (TX_ID)
GROUP BY
  CUSTOMER_ID
HAVING
  SAFE_DIVIDE(
    SUM(IF(TX_FRAUD IS NOT NULL, TX_FRAUD, 0)),
    SUM(IF(TX_FRAUD IS NOT NULL, 1, 0))
    ) > 0.5
ORDER BY
  PCT_TX_FRAUD DESC
LIMIT 10;

-- Part 4: Terminal Analysis (15 mins)

-- TODO: Calculate terminal-level metrics
-- Hint: Similar to customer analysis but for terminals
SELECT
  TERMINAL_ID,
  ... AS NUM_TX,
  ... AS AVG_TX_AMOUNT,
  SAFE_DIVIDE(
    SUM(IF(TX_FRAUD IS NOT NULL, TX_FRAUD, 0)),
    SUM(IF(TX_FRAUD IS NOT NULL, 1, 0))
    ) AS PCT_TX_FRAUD
FROM
  tx.tx
LEFT JOIN
  tx.txlabels USING (TX_ID)
GROUP BY
  TERMINAL_ID;

-- Bonus Challenges (if time permits):

-- TODO: Analyze time-based patterns
-- Hint: Extract hour/day from TX_TS
SELECT
  ... as hour,
  ... AS NUM_TX,
  ... AS PCT_TX_FRAUD
FROM
  tx.tx
LEFT JOIN
  tx.txlabels USING (TX_ID)
GROUP BY
  1
ORDER BY
  1;

-- TODO: Find suspicious amount patterns
-- Hint: Look for frequently repeated amounts
SELECT
  TX_AMOUNT,
  ... as frequency,
  ... AS PCT_TX_FRAUD
FROM
  tx.tx
LEFT JOIN
  tx.txlabels USING (TX_ID)
GROUP BY
  TX_AMOUNT
HAVING
  COUNT(*) > 10
ORDER BY
  frequency DESC
LIMIT 10;
