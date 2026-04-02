import os
import pandas as pd
import streamlit as st          # web dashboard framework
import plotly.express as px     # interactive charts
import plotly.graph_objects as go  # advanced chart control

BASE = r"C:\Users\Admin\Documents\Project\Python Project\ETL Project\Digital_Marketing_Keyword"

# Page config — must be first Streamlit command
st.set_page_config(
    page_title="Keyword Analytics Dashboard",
    page_icon="📈",
    layout="wide"
)

# Title
st.title("📈 Digital Marketing Keyword Analytics")
st.markdown("Live keyword search trends powered by Google Trends + PySpark + Delta Lake")

# Load data from Silver CSV (simpler than reading Delta directly)
@st.cache_data  # cache so it doesn't reload on every interaction
def load_data():
    path = os.path.join(BASE, "raw", "keyword_trends.csv")
    df = pd.read_csv(path, parse_dates=["week"])
    df = df.rename(columns={
        "digital marketing":        "digital_marketing",
        "social media marketing":   "social_media_marketing",
        "content marketing":        "content_marketing",
        "email marketing":          "email_marketing"
    })
    return df

df = load_data()

# Load related queries
@st.cache_data
def load_related():
    path = os.path.join(BASE, "raw", "related_queries.csv")
    return pd.read_csv(path)

df_related = load_related()

KEYWORDS = ["SEO", "digital_marketing", "social_media_marketing",
            "content_marketing", "email_marketing"]

# ── Sidebar filters ───────────────────────────────────────────────
st.sidebar.header("Filters")

selected_keywords = st.sidebar.multiselect(
    "Select keywords to display",
    options=KEYWORDS,
    default=KEYWORDS         # all selected by default
)

date_range = st.sidebar.date_input(
    "Date range",
    value=[df["week"].min(), df["week"].max()]
)

# Filter data
df_filtered = df[
    (df["week"] >= pd.Timestamp(date_range[0])) &
    (df["week"] <= pd.Timestamp(date_range[1]))
]

# ── KPI cards row ─────────────────────────────────────────────────
st.subheader("Key Metrics")
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total days tracked",   len(df_filtered))
col2.metric("Peak SEO score",       df_filtered["SEO"].max())
col3.metric("Avg SEO score",        round(df_filtered["SEO"].mean(), 1))
col4.metric("Top keyword",          "SEO")

st.divider()

# ── Line chart — trends over time ────────────────────────────────
st.subheader("Search Interest Over Time")

if selected_keywords:
    df_melt = df_filtered[["week"] + selected_keywords].melt(
        id_vars="week",
        var_name="keyword",
        value_name="interest"
    )
    fig_line = px.line(
        df_melt,
        x="week",
        y="interest",
        color="keyword",
        title="Weekly search interest (0-100 scale)",
        labels={"interest": "Search interest", "week": "Date"}
    )
    fig_line.update_layout(height=400)
    st.plotly_chart(fig_line, use_container_width=True)
else:
    st.warning("Please select at least one keyword from the sidebar")

# ── Two column layout ─────────────────────────────────────────────
col_left, col_right = st.columns(2)

# Bar chart — average interest per keyword
with col_left:
    st.subheader("Average Interest by Keyword")
    avg_data = df_filtered[KEYWORDS].mean().reset_index()
    avg_data.columns = ["keyword", "avg_interest"]
    avg_data = avg_data.sort_values("avg_interest", ascending=False)

    fig_bar = px.bar(
        avg_data,
        x="keyword",
        y="avg_interest",
        color="keyword",
        title="Average search interest per keyword",
        labels={"avg_interest": "Avg interest"}
    )
    fig_bar.update_layout(showlegend=False, height=350)
    st.plotly_chart(fig_bar, use_container_width=True)

# Pie chart — share of total interest
with col_right:
    st.subheader("Share of Total Search Interest")
    total_data = df_filtered[KEYWORDS].sum().reset_index()
    total_data.columns = ["keyword", "total_interest"]

    fig_pie = px.pie(
        total_data,
        names="keyword",
        values="total_interest",
        title="Each keyword's share of total searches"
    )
    fig_pie.update_layout(height=350)
    st.plotly_chart(fig_pie, use_container_width=True)

st.divider()

# ── Related queries table ─────────────────────────────────────────
st.subheader("Top Related Queries")

selected_kw = st.selectbox(
    "Select keyword to see related queries",
    options=["SEO", "digital marketing", "social media marketing",
             "content marketing", "email marketing"]
)

related_filtered = df_related[
    df_related["source_keyword"] == selected_kw
].head(10)

st.dataframe(
    related_filtered[["query", "value"]].reset_index(drop=True),
    use_container_width=True
)

st.divider()

# ── Raw data table ────────────────────────────────────────────────
with st.expander("View raw data"):
    st.dataframe(df_filtered, use_container_width=True)

st.caption("Data source: Google Trends via pytrends | Processed with PySpark | Stored in Delta Lake")