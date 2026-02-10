import streamlit as st
from snowflake.snowpark import Session

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Procurement Analysis",
    layout="wide"
)

st.title("📊 Procurement Analysis")

# -----------------------------
# Snowflake Connection
# -----------------------------
session = Session.builder.configs(
    st.secrets["snowflake"]
).create()

# =============================
# FILTERS (Year & Month)
# =============================
st.sidebar.header("Filters")

year_df = session.sql("""
    SELECT DISTINCT YEAR(transaction_date) AS year
    FROM FACT_PROCUREMENT_SPEND
    ORDER BY year
""").to_pandas()

years = year_df["YEAR"].tolist()
selected_year = st.sidebar.selectbox("Select Year", years)

month_df = session.sql(f"""
    SELECT DISTINCT MONTH(transaction_date) AS month
    FROM FACT_PROCUREMENT_SPEND
    WHERE YEAR(transaction_date) = {selected_year}
    ORDER BY month
""").to_pandas()

months = month_df["MONTH"].tolist()

month_names = {
    1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun",
    7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"
}

selected_month = st.sidebar.selectbox(
    "Select Month",
    months,
    format_func=lambda x: month_names[x]
)

st.write("Selected Year:", selected_year)
st.write("Selected Month:", selected_month)


# =============================
# MONTHLY SPEND
# =============================
st.header("📅 Daily Spend Trend")

monthly_q = f"""
SELECT
    DATE(transaction_date) AS day,
    SUM(net_amount) AS daily_spend
FROM FACT_PROCUREMENT_SPEND
WHERE YEAR(transaction_date) = {selected_year}
  AND MONTH(transaction_date) = {selected_month}
GROUP BY day
ORDER BY day
"""
df_month = session.sql(monthly_q).to_pandas()

st.line_chart(df_month, x="DAY", y="DAILY_SPEND")

# =============================
# CATEGORY WISE SPEND
# =============================
st.header("📦 Category Wise Spend")

category_q = f"""
SELECT category, SUM(net_amount) AS total_spend
FROM FACT_PROCUREMENT_SPEND
WHERE YEAR(transaction_date) = {selected_year}
  AND MONTH(transaction_date) = {selected_month}
GROUP BY category
ORDER BY total_spend DESC
"""
df_cat = session.sql(category_q).to_pandas()

st.bar_chart(df_cat, x="CATEGORY", y="TOTAL_SPEND")

# =============================
# CITY WISE SPEND
# =============================
st.header("🏙 City Wise Spend")

city_q = f"""
SELECT city, SUM(net_amount) AS total_spend
FROM FACT_PROCUREMENT_SPEND
WHERE YEAR(transaction_date) = {selected_year}
  AND MONTH(transaction_date) = {selected_month}
GROUP BY city
ORDER BY total_spend DESC
"""
df_city = session.sql(city_q).to_pandas()
st.bar_chart(df_city, x="CITY", y="TOTAL_SPEND")


# =============================
# VENDOR WISE SPEND
# =============================
st.header("🏭 Vendor Wise Spend")

vendor_q = f"""
SELECT v.vendor_name, SUM(f.net_amount) AS vendor_spend
FROM FACT_PROCUREMENT_SPEND f
JOIN DIM_VENDOR v
  ON f.vendor_id = v.vendor_id
WHERE YEAR(f.transaction_date) = {selected_year}
  AND MONTH(f.transaction_date) = {selected_month}
GROUP BY v.vendor_name
ORDER BY vendor_spend DESC
"""
df_vendor = session.sql(vendor_q).to_pandas()
st.bar_chart(df_vendor, x="VENDOR_NAME", y="VENDOR_SPEND")


# =============================
# DISCOUNT ANALYSIS
# =============================
st.header("💸 Average Discount by Category")

discount_q = f"""
SELECT category, AVG(discount_amount) AS avg_discount
FROM FACT_PROCUREMENT_SPEND
WHERE YEAR(transaction_date) = {selected_year}
  AND MONTH(transaction_date) = {selected_month}
GROUP BY category
"""
df_discount = session.sql(discount_q).to_pandas()
st.bar_chart(df_discount, x="CATEGORY", y="AVG_DISCOUNT")


# =============================
# YEAR WISE SPEND
# =============================
st.header("📆 Year Wise Spend")

year_q = f"""
SELECT
    YEAR(transaction_date) AS year,
    SUM(net_amount) AS total_spend
FROM FACT_PROCUREMENT_SPEND
WHERE YEAR(transaction_date) = {selected_year}
GROUP BY year
"""
df_year = session.sql(year_q).to_pandas()
st.bar_chart(df_year, x="YEAR", y="TOTAL_SPEND")


# =============================
# AVERAGE ANALYSIS
# =============================
st.header("📈 Average Quantity & Spend")

avg_q = f"""
SELECT
    category,
    ROUND(AVG(quantity), 2) AS avg_quantity_per_txn,
    ROUND(AVG(net_amount), 2) AS avg_spend_per_txn
FROM FACT_PROCUREMENT_SPEND
WHERE YEAR(transaction_date) = {selected_year}
  AND MONTH(transaction_date) = {selected_month}
  AND category IS NOT NULL
GROUP BY category
"""
df_avg = session.sql(avg_q).to_pandas()
st.dataframe(df_avg)






