import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account

# -------------------------
# CONFIG
# -------------------------
KEY_PATH = st.secrets["KEY_PATH"]
PROJECT_ID = st.secrets["PROJECT_ID"]
VIEW_ID = "horseracing-pacey32-github.bettingapp.3_BetSelections_Enriched"

GREEN_COLOUR = "#8FA58C"
AMBER_COLOUR = "#D8B26E"

# -------------------------
# PAGE CONFIG
# -------------------------
st.set_page_config(page_title="Historic Performance", layout="wide")

# -------------------------
# CSS
# -------------------------
def load_css(file_name="styles.css"):
    with open(file_name, "r") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css()

# -------------------------
# BIGQUERY
# -------------------------
@st.cache_resource
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

bq_client = get_bigquery_client()

@st.cache_data(ttl=60)
def load_bet_data():
    query = f"""
        SELECT *
        FROM `{VIEW_ID}`
        ORDER BY race_date DESC, race_time
    """
    df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)

    if df.empty:
        return df

    if "race_date" in df.columns:
        df["race_date"] = pd.to_datetime(df["race_date"]).dt.date

    numeric_cols = [
        "stake",
        "Actual_Win_Return",
        "Actual_Place_Return",
        "Actual_Total_Return",
        "Place_Returns"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[col] = 0.0

    # Clean text columns used in hover text
    for col in ["race_time", "race_location", "horse_name"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
        else:
            df[col] = ""

    return df

# -------------------------
# APP
# -------------------------
st.title("Historic Performance")

df = load_bet_data()

if df.empty:
    st.warning("No performance data found.")
    st.stop()

# -------------------------
# FILTERS
# -------------------------
metric_options = [
    "# Horses",
    "£ Staked",
    "£ Return",
    "£ Profit",
    "# Wins",
    "# Wins & Places",
]

col1, col2 = st.columns([2, 1])

with col1:
    selected_metric_label = st.selectbox("Metric", metric_options)

with col2:
    ew_selector = st.selectbox("Each Way", ["N", "Y"])

# -------------------------
# RETURN LOGIC
# -------------------------
if ew_selector == "Y":
    df["selected_return_row"] = df["Actual_Total_Return"]
else:
    df["selected_return_row"] = df["Actual_Win_Return"]

# -------------------------
# DAILY SUMMARY
# -------------------------
daily_summary = (
    df.groupby("race_date", dropna=False)
    .agg(
        horses=("runner_id", "count"),
        total_staked=("stake", "sum"),
        selected_return=("selected_return_row", "sum"),
        wins=("Actual_Win_Return", lambda s: (s > 0).sum()),
        wins_places=("Place_Returns", lambda s: (s > 0).sum()),
    )
    .reset_index()
    .sort_values("race_date")
)

# Correct profit logic:
# daily profit = daily return - daily total staked
daily_summary["profit"] = daily_summary["selected_return"] - daily_summary["total_staked"]

# -------------------------
# METRIC MAP
# -------------------------
metric_map = {
    "# Horses": ("horses", "# Horses"),
    "£ Staked": ("total_staked", "£ Staked"),
    "£ Return": ("selected_return", "£ Return"),
    "£ Profit": ("profit", "£ Profit"),
    "# Wins": ("wins", "# Wins"),
    "# Wins & Places": ("wins_places", "# Wins & Places"),
}

selected_metric, selected_title = metric_map[selected_metric_label]

# -------------------------
# TOTALS TABLE
# -------------------------
totals_df = pd.DataFrame(
    {
        "Metric": [
            "# Horses",
            "£ Staked",
            "£ Return",
            "£ Profit",
            "# Wins",
            "# Wins & Places",
        ],
        "Value": [
            int(daily_summary["horses"].sum()),
            round(daily_summary["total_staked"].sum(), 2),
            round(daily_summary["selected_return"].sum(), 2),
            round(daily_summary["profit"].sum(), 2),
            int(daily_summary["wins"].sum()),
            int(daily_summary["wins_places"].sum()),
        ],
    }
)

# -------------------------
# HOVER TEXT FOR PLOT
# -------------------------
hover_per_day = (
    df.sort_values(["race_date", "race_time", "race_location", "horse_name"])
      .groupby("race_date", dropna=False)
      .apply(
          lambda x: "<br>".join(
              x.apply(
                  lambda r: f"{r['race_time']} | {r['race_location']} | {r['horse_name']}",
                  axis=1
              ).tolist()
          )
      )
      .reset_index(name="hover_text")
)

chart_df = daily_summary.merge(hover_per_day, on="race_date", how="left")
chart_df["race_date"] = pd.to_datetime(chart_df["race_date"])

# -------------------------
# LAYOUT
# -------------------------
left_col, right_col = st.columns([2, 1])

with left_col:
    st.markdown("### Performance Over Time")

    if selected_metric == "profit":
        bar_colours = [
            AMBER_COLOUR if value < 0 else GREEN_COLOUR
            for value in chart_df[selected_metric]
        ]
    else:
        bar_colours = [GREEN_COLOUR] * len(chart_df)

    fig = go.Figure(
        data=[
            go.Bar(
                x=chart_df["race_date"],
                y=chart_df[selected_metric],
                marker_color=bar_colours,
                customdata=chart_df["hover_text"],
                hovertemplate=(
                    "<b>%{x|%d %b %Y}</b><br><br>"
                    "%{customdata}<extra></extra>"
                ),
            )
        ]
    )

    fig.update_layout(
        height=400,
        xaxis_title="Race Date",
        yaxis_title=selected_title,
        hoverlabel=dict(
            bgcolor="white",
            font_size=13,
            font_family="Inter"
        ),
        showlegend=False,
        bargap=0.15,
    )

    fig.update_xaxes(
        tickformat="%d %b %Y",
        tickangle=-35
    )

    st.plotly_chart(fig, use_container_width=True)

with right_col:
    st.markdown("### Totals")
    st.dataframe(
        totals_df,
        use_container_width=True,
        hide_index=True
    )

# -------------------------
# DAILY SUMMARY TABLE
# -------------------------
st.markdown("<hr>", unsafe_allow_html=True)
st.markdown("### Daily Summary")

display_daily = daily_summary.copy().rename(
    columns={
        "race_date": "Race Date",
        "horses": "# Horses",
        "total_staked": "£ Staked",
        "selected_return": "£ Return",
        "profit": "£ Profit",
        "wins": "# Wins",
        "wins_places": "# Wins & Places",
    }
)

st.dataframe(
    display_daily.sort_values("Race Date", ascending=False),
    use_container_width=True,
    hide_index=True
)