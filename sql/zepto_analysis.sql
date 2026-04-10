-- ─────────────────────────────────────────────
-- PURPOSE: 10 business analytics queries
-- Each query answers a real business question
-- ─────────────────────────────────────────────


-- ── QUERY 1: Revenue ranking by hub ──────────
-- Business question: Which hubs make us the most money?
-- Concept: GROUP BY + SUM + ORDER BY

SELECT
    h.hub_name,
    h.tier,
    COUNT(DISTINCT t.user_id)    AS unique_buyers,
    SUM(t.amount)                AS total_revenue,
    ROUND(AVG(t.amount), 2)      AS avg_txn_value,
    RANK() OVER (ORDER BY SUM(t.amount) DESC) AS revenue_rank
FROM transactions t
JOIN hubs h ON t.hub_id = h.hub_id
WHERE t.txn_type = 'purchase'
  AND t.status   = 'completed'
GROUP BY h.hub_name, h.tier
ORDER BY total_revenue DESC;


-- ── QUERY 2: Wallet bucket analysis ──────────
-- Business question: How are users distributed across wallet tiers?
-- Concept: CASE WHEN to create custom categories

SELECT
    CASE
        WHEN wallet_balance >= 1000 THEN 'High Value (₹1000+)'
        WHEN wallet_balance >= 300  THEN 'Mid Value (₹300-999)'
        WHEN wallet_balance >= 50   THEN 'Low Value (₹50-299)'
        ELSE                             'Minimal (<₹50)'
    END                          AS wallet_tier,
    COUNT(*)                     AS user_count,
    ROUND(AVG(wallet_balance),2) AS avg_balance,
    SUM(wallet_balance)          AS total_balance,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_users
FROM users
GROUP BY wallet_tier
ORDER BY avg_balance DESC;


-- ── QUERY 3: Churn cohorts (30/60/90 days) ───
-- Business question: How many users are at each inactivity stage?
-- Concept: CASE WHEN on numeric column

SELECT
    CASE
        WHEN days_inactive <= 30  THEN '0-30 days (Active)'
        WHEN days_inactive <= 60  THEN '31-60 days (At Risk)'
        WHEN days_inactive <= 90  THEN '61-90 days (Dormant)'
        WHEN days_inactive <= 180 THEN '91-180 days (Hibernating)'
        ELSE                           '180+ days (Lost)'
    END                   AS inactivity_bucket,
    COUNT(*)              AS user_count,
    ROUND(AVG(wallet_balance), 2) AS avg_wallet,
    SUM(wallet_balance)   AS total_wallet_at_risk
FROM users
GROUP BY inactivity_bucket
ORDER BY MIN(days_inactive);


-- ── QUERY 4: Monthly revenue trend ───────────
-- Business question: Is revenue growing month over month?
-- Concept: GROUP BY on date part, LAG window function

WITH monthly_revenue AS (
    SELECT
        txn_month,
        SUM(amount) AS revenue
    FROM transactions
    WHERE txn_type = 'purchase'
      AND status   = 'completed'
    GROUP BY txn_month
)
SELECT
    txn_month,
    revenue,
    LAG(revenue) OVER (ORDER BY txn_month) AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY txn_month))
        * 100.0
        / NULLIF(LAG(revenue) OVER (ORDER BY txn_month), 0)
    , 1) AS mom_growth_pct
FROM monthly_revenue
ORDER BY txn_month;


-- ── QUERY 5: Top 10 highest value customers ──
-- Business question: Who are our most valuable users?
-- Concept: Window function RANK()

SELECT
    u.user_id,
    u.hub_id,
    u.customer_segment,
    u.wallet_balance,
    u.total_recharges,
    u.days_inactive,
    RANK() OVER (ORDER BY u.wallet_balance DESC) AS value_rank
FROM users u
WHERE u.is_churned = 0
ORDER BY value_rank
LIMIT 10;


-- ── QUERY 6: SKU salience by hub ─────────────
-- Business question: Which products sell best in each hub?
-- Concept: RANK() OVER PARTITION BY (rank within each group)

WITH sku_hub_sales AS (
    SELECT
        t.hub_id,
        t.sku_id,
        s.product_name,
        s.category,
        COUNT(*)       AS txn_count,
        SUM(t.amount)  AS revenue
    FROM transactions t
    JOIN skus s ON t.sku_id = s.sku_id
    WHERE t.txn_type = 'purchase'
    GROUP BY t.hub_id, t.sku_id, s.product_name, s.category
),
ranked AS (
    SELECT *,
        RANK() OVER (PARTITION BY hub_id ORDER BY revenue DESC) AS rank_in_hub
    FROM sku_hub_sales
)
SELECT hub_id, product_name, category, revenue, txn_count, rank_in_hub
FROM ranked
WHERE rank_in_hub <= 3
ORDER BY hub_id, rank_in_hub;


-- ── QUERY 7: Cohort retention ─────────────────
-- Business question: Do users who joined in certain months stay longer?
-- Concept: DATE_TRUNC to group by registration month

SELECT
    DATE_TRUNC('month', registration_date) AS cohort_month,
    COUNT(*)                               AS total_users,
    SUM(CASE WHEN is_churned = 0 THEN 1 ELSE 0 END) AS active_users,
    ROUND(
        SUM(CASE WHEN is_churned = 0 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 1
    )                                      AS retention_pct
FROM users
GROUP BY cohort_month
ORDER BY cohort_month;


-- ── QUERY 8: Debit anomaly detection ─────────
-- Business question: Which users have suspiciously high debit activity?
-- Concept: HAVING clause to filter after aggregation

SELECT
    user_id,
    COUNT(*)                                              AS total_txns,
    SUM(CASE WHEN txn_type='debit_transfer' THEN 1 ELSE 0 END) AS debit_count,
    ROUND(
        SUM(CASE WHEN txn_type='debit_transfer' THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 1
    )                                                     AS debit_pct
FROM transactions
WHERE status = 'completed'
GROUP BY user_id
HAVING
    SUM(CASE WHEN txn_type='debit_transfer' THEN 1 ELSE 0 END) * 1.0
    / COUNT(*) > 0.5
ORDER BY debit_pct DESC
LIMIT 20;


-- ── QUERY 9: Revenue at risk from churn ───────
-- Business question: How much wallet money could we lose?
-- Concept: JOIN users + filter on churn score

SELECT
    customer_segment,
    COUNT(*)             AS at_risk_users,
    SUM(wallet_balance)  AS wallet_revenue_at_risk,
    ROUND(AVG(wallet_balance), 2) AS avg_wallet
FROM users
WHERE is_churned = 1
GROUP BY customer_segment
ORDER BY wallet_revenue_at_risk DESC;


-- ── QUERY 10: Hub month-over-month growth ─────
-- Business question: Which hubs are growing vs declining?
-- Concept: CTEs + LAG partitioned by hub

WITH hub_monthly AS (
    SELECT
        hub_id,
        txn_month,
        SUM(amount) AS revenue
    FROM transactions
    WHERE txn_type = 'purchase'
      AND status   = 'completed'
    GROUP BY hub_id, txn_month
)
SELECT
    hub_id,
    txn_month,
    revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (PARTITION BY hub_id ORDER BY txn_month))
        * 100.0
        / NULLIF(LAG(revenue) OVER (PARTITION BY hub_id ORDER BY txn_month), 0)
    , 1) AS hub_mom_growth_pct
FROM hub_monthly
ORDER BY hub_id, txn_month;