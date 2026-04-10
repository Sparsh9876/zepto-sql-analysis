# 🛒 Zepto Product Analytics — SQL Case Study

> **Analysing 3,700+ SKUs across 14 product categories to uncover pricing gaps,
> inventory risk, and revenue opportunities for a quick-commerce platform**

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat&logo=postgresql&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Advanced-blue?style=flat)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen?style=flat)
![Domain](https://img.shields.io/badge/Domain-E--Commerce-orange?style=flat)

---

## 🎯 Business Problem

Zepto operates a 10-minute grocery delivery model where **pricing accuracy,
discount strategy, and stock availability directly impact revenue**.
This project analyses their product catalogue to answer 8 business questions
that a Category Manager or Growth Analyst would face on day one.

---

## 📦 Dataset

| Property | Detail |
|---|---|
| Source | Zepto product catalogue (public dataset) |
| Records | 3,732 SKUs (after cleaning) |
| Categories | 14 product categories |
| Key Fields | MRP, Discount %, Discounted Price, Weight, Stock Status, Quantity |

---

## 🔧 Tools Used

| Tool | Purpose |
|---|---|
| PostgreSQL 15 | Database creation, data cleaning, all queries |
| pgAdmin 4 | Query execution and result exploration |
| Excel | Raw data formatting before SQL import |

---

## 🗂️ Project Structure

```
zepto-sql-analysis/
│
├── data/
│   ├── zepto_raw_data.csv         ← Original dataset (3,732 rows)
│   └── zepto_cleaned_data.csv     ← After NULL removal + price correction
│
├── sql/
│   └── zepto_analysis.sql         ← Complete SQL file (schema + cleaning + 8 queries)
│
├── assets/
│   └── query_results/             ← Screenshots of key query outputs
│
└── README.md
```

---

## 🧹 Data Cleaning Steps

```sql
-- 1. Removed products where MRP = 0 (invalid records)
DELETE FROM zepto WHERE mrp = 0;

-- 2. Converted prices from paise → rupees (source data in paise)
UPDATE zepto
SET mrp = mrp / 100.0,
    discountedSellingPrice = discountedSellingPrice / 100.0;
```

**Null check result:** 0 null values found across all critical columns —
dataset was structurally clean before price correction.

---

## 📊 10 business analytics queries - Each query answers a real business question

### Q1 — QUERY 1: Revenue ranking by hub ──────────
> *Business question: Which hubs make us the most money?-- Concept: GROUP BY + SUM + ORDER BY*

```sql
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
```

---

### Q2 — QUERY 2: Wallet bucket analysis ──────────
> *Business question: How are users distributed across wallet tiers? --Concept: CASE WHEN to create custom categories*

```sql
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
```

---

### Q3 — QUERY 3: Churn cohorts (30/60/90 days) ───
> *How many users are at each inactivity stage?--Concept: CASE WHEN on numeric column*

```sql
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

```
---

### Q4 — Monthly revenue trend ───────────
> *Is revenue growing month over month?--Concept: GROUP BY on date part, LAG window function*

```sql
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
```
---

### Q5 — Top 10 highest value customers ──
> *Who are our most valuable users?--Concept: Window function RANK()*

```sql
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
```
---

### Q6 —  SKU salience by hub ───────────── 
> *Which products sell best in each hub?--Concept: RANK() OVER PARTITION BY (rank within each group)*

```sql
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
```
---

### Q7 — Cohort retention ─────────────────
> *Do users who joined in certain months stay longer?--Concept: DATE_TRUNC to group by registration month*

```sql
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
```
---

### Q8 — Debit anomaly detection ─────────
> *Which users have suspiciously high debit activity?--Concept: HAVING clause to filter after aggregation*

```sql
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
```
---
### Q9 —Revenue at risk from churn ───────
> *How much wallet money could we lose?--Concept: JOIN users + filter on churn score*

```sql
SELECT
    customer_segment,
    COUNT(*)             AS at_risk_users,
    SUM(wallet_balance)  AS wallet_revenue_at_risk,
    ROUND(AVG(wallet_balance), 2) AS avg_wallet
FROM users
WHERE is_churned = 1
GROUP BY customer_segment
ORDER BY wallet_revenue_at_risk DESC;
```
---

### Q10 — Hub month-over-month growth ─────
> *Which hubs are growing vs declining?--Concept: CTEs + LAG partitioned by hub*

```sql
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
```
---


## 💡 Key Business Insights

| Insight | Detail |
|---|---|
| **12.1% of SKUs are out of stock** | 453 of 3,732 products unavailable — direct lost sales |
| **Cooking Essentials is #1 revenue category** | Highest estimated revenue across all 14 categories |
| **Discount range: 0–51%** | Wide variance — no consistent discount strategy |
| **Premium products underpromoted** | ₹500+ items with <10% discount could drive upsell with better offers |
| **Fresh produce = best price-per-gram value** | Competitive moat in staples category |

---

## 🚀 How to Run

```bash
# 1. Create database
createdb zepto_db

# 2. Open pgAdmin → Query Tool → run the SQL file
# File: sql/zepto_analysis.sql

# 3. Run sections in order:
#    - Schema creation (DROP + CREATE TABLE)
#    - Data import (COPY or pgAdmin import wizard)
#    - Cleaning queries (DELETE + UPDATE)
#    - Business queries (Q1 through Q8)
```

---

## 👤 Author

**Sparsh Bhatnagar** — Data Analytics | MCA (ML & AI), Amity University
[LinkedIn](https://www.linkedin.com/in/sparshbhatnagar/) | Ghaziabad, India
[![Email](https://img.shields.io/badge/Email-Reach%20Me-D14836?style=flat&logo=gmail&logoColor=white)](mailto:Sparsh.Bhatnagar13@gmail.com)