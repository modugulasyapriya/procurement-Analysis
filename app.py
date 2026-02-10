import streamlit as st
from snowflake.snowpark.context import get_active_session

st.title(" Procurement Spend Dashboard")

# Snowflake session
session = get_active_session()

# -----------------------------
# Monthly Spend Query
# -----------------------------
query = """
SELECT
    DATE_TRUNC('MONTH', transaction_date) AS month,
    SUM(net_amount) AS monthly_spend
FROM GOLD.FACT_PROCUREMENT_SPEND
GROUP BY month
ORDER BY month
"""

df = session.sql(query).to_pandas()

# -----------------------------
# Show table
# -----------------------------
st.subheader("Monthly Spend Data")
st.dataframe(df, use_container_width=True)

# -----------------------------
# Show graph
# -----------------------------
st.subheader("Monthly Spend Trend")
st.line_chart(
    data=df,
    x="MONTH",
    y="MONTHLY_SPEND"
)






query_category = """

SELECT
    category,
    SUM(net_amount) AS total_spend
FROM GOLD.FACT_PROCUREMENT_SPEND
GROUP BY category
ORDER BY total_spend DESC
"""

df_cat = session.sql(query_category).to_pandas()

st.subheader("Category Wise Spend")
st.bar_chart(
    data=df_cat,
    x="CATEGORY",
    y="TOTAL_SPEND"
)







# =========================
# CITY WISE SPEND
# =========================
st.header(" City Wise Spend")

city_q = """
SELECT city, SUM(net_amount) AS total_spend
FROM GOLD.FACT_PROCUREMENT_SPEND
GROUP BY city
ORDER BY total_spend DESC
"""
df_city = session.sql(city_q).to_pandas()

st.bar_chart(df_city, x="CITY", y="TOTAL_SPEND")
st.dataframe(df_city, use_container_width=True)






# =========================
# VENDOR WISE SPEND
# =========================
st.header("🏭 Vendor Wise Spend")

vendor_q = """
SELECT v.vendor_name, SUM(f.net_amount) AS vendor_spend
FROM GOLD.FACT_PROCUREMENT_SPEND f
JOIN GOLD.DIM_VENDOR v
ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name
ORDER BY vendor_spend DESC
"""
df_vendor = session.sql(vendor_q).to_pandas()

st.bar_chart(df_vendor, x="VENDOR_NAME", y="VENDOR_SPEND")
st.dataframe(df_vendor, use_container_width=True)






# =========================
# DISCOUNT ANALYSIS
# =========================
st.header("💸 Average Discount by Category")

discount_q = """
SELECT category, AVG(discount_amount) AS avg_discount
FROM GOLD.FACT_PROCUREMENT_SPEND
GROUP BY category
"""
df_discount = session.sql(discount_q).to_pandas()

st.bar_chart(df_discount, x="CATEGORY", y="AVG_DISCOUNT")
st.dataframe(df_discount, use_container_width=True)






# =========================
# MONTH × VENDOR × CATEGORY
# =========================
st.header(" Month × Vendor × Category Analysis")

mvc_q = """
SELECT
  TO_CHAR(transaction_date, 'YYYY-MM') AS month,
  vendor_id,
  category,
  SUM(quantity) AS total_quantity,
  SUM(net_amount) AS total_spend
FROM GOLD.FACT_PROCUREMENT_SPEND
GROUP BY month, vendor_id, category
ORDER BY month, total_spend DESC
"""
df_mvc = session.sql(mvc_q).to_pandas()

st.line_chart(df_mvc, x="MONTH", y="TOTAL_SPEND")
st.dataframe(df_mvc, use_container_width=True)








# =========================
# YEAR WISE SPEND
# =========================
st.header("Year Wise Spend")

year_q = """
SELECT
  YEAR(transaction_date) AS year,
  SUM(net_amount) AS total_spend
FROM GOLD.FACT_PROCUREMENT_SPEND
GROUP BY year
ORDER BY year
"""
df_year = session.sql(year_q).to_pandas()

st.line_chart(df_year, x="YEAR", y="TOTAL_SPEND")
st.dataframe(df_year, use_container_width=True)






# =========================
# YEAR × CATEGORY CONSOLIDATED
# =========================
st.header(" Year × Category Consolidated Spend")

year_cat_q = """
SELECT
  YEAR(transaction_date) AS year,
  category,
  SUM(quantity) AS total_quantity,
  SUM(net_amount) AS total_spend
FROM GOLD.FACT_PROCUREMENT_SPEND
GROUP BY year, category
ORDER BY year, total_spend DESC
"""
df_year_cat = session.sql(year_cat_q).to_pandas()

st.bar_chart(df_year_cat, x="CATEGORY", y="TOTAL_SPEND")
st.dataframe(df_year_cat, use_container_width=True)






# =========================
# AVERAGES ANALYSIS
# =========================
st.header("📈 Average Quantity & Spend")

avg_q = """
SELECT
  YEAR(transaction_date) AS year,
  category,
  ROUND(AVG(quantity), 2) AS avg_quantity_per_txn,
  ROUND(AVG(net_amount), 2) AS avg_spend_per_txn
FROM GOLD.FACT_PROCUREMENT_SPEND
WHERE category IS NOT NULL
GROUP BY year, category
ORDER BY year
"""
df_avg = session.sql(avg_q).to_pandas()

st.line_chart(df_avg, x="YEAR", y="AVG_SPEND_PER_TXN")
st.dataframe(df_avg, use_container_width=True)