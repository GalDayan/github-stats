import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

st.title("GitHub Metrics Dashboard")

@st.cache_data
def load_data():
    commits_df = pd.read_csv("commits_data_all.csv", parse_dates=["date"])
    prs_df = pd.read_csv("prs_data_all.csv", parse_dates=["created_at", "closed_at"])
    return commits_df, prs_df

commits_df, prs_df = load_data()

# Sidebar repository selection with default "All" option
repository_option = st.sidebar.selectbox(
    "Select Repository",
    options=["All"] + sorted(commits_df["repository"].unique()),
    index=0
)

if repository_option != "All":
    commits_df = commits_df[commits_df["repository"] == repository_option]
    prs_df = prs_df[prs_df["repository"] == repository_option]

# Sidebar options for time interval, metric breakdown, and math function type
interval = st.sidebar.selectbox(
    "Select time interval", options=["Daily", "Weekly", "Byweekly", "Monthly"], index=2
)
metric_breakdown = st.sidebar.selectbox(
    "Metric for Contributor Breakdown", options=["Commits", "Lines Changed"]
)
math_function = st.sidebar.selectbox(
    "Select display type",
    options=[
        "Total", "Cumulative", "Delta", "Percentage Change", 
        "Delta from the Average", "Signed Log Delta",
        "Power Transform", "Exponential Transform", "Log Transform"
    ],
    index=0
)

# For Power Transform, let user select the exponent
if math_function == "Power Transform":
    exponent = st.sidebar.slider("Select power exponent (p > 1)", min_value=1.1, max_value=5.0, value=2.0, step=0.1)

# Resample commits data based on the selected interval
if interval == "Daily":
    commits_resampled = (
        commits_df.set_index("date")
        .resample("D")
        .agg({"sha": "count"})
        .rename(columns={"sha": "Commits"})
    )
elif interval == "Byweekly":
    commits_resampled = (
        commits_df.set_index("date")
        .resample("2W")
        .agg({"sha": "count"})
        .rename(columns={"sha": "Commits"})
    )
elif interval == "Weekly":
    commits_resampled = (
        commits_df.set_index("date")
        .resample("W")
        .agg({"sha": "count"})
        .rename(columns={"sha": "Commits"})
    )
else:  # Monthly
    commits_resampled = (
        commits_df.set_index("date")
        .resample("M")
        .agg({"sha": "count"})
        .rename(columns={"sha": "Commits"})
    )

# Apply selected math function for commits data
if math_function == "Cumulative":
    commits_final = commits_resampled.cumsum()
elif math_function == "Delta":
    commits_final = commits_resampled.diff()
elif math_function == "Percentage Change":
    commits_final = commits_resampled.pct_change() * 100
elif math_function == "Delta from the Average":
    commits_final = commits_resampled - commits_resampled.mean()
elif math_function == "Signed Log Delta":
    commits_delta = commits_resampled.diff()
    commits_final = commits_delta.apply(lambda x: np.sign(x) * np.log1p(abs(x)))
elif math_function == "Power Transform":
    commits_final = commits_resampled ** exponent
elif math_function == "Exponential Transform":
    commits_final = np.exp(commits_resampled)
elif math_function == "Log Transform":
    commits_final = np.log1p(commits_resampled)
else:
    commits_final = commits_resampled

st.subheader(f"Commits ({interval}) ({math_function})")
st.line_chart(commits_final)

# Resample PR data by creation date for PRs created chart
if interval == "Daily":
    prs_resampled = (
        prs_df.set_index("created_at")
        .resample("D")
        .count()[["pr_number"]]
        .rename(columns={"pr_number": "PRs Created"})
    )
elif interval == "Byweekly":
    prs_resampled = (
        prs_df.set_index("created_at")
        .resample("2W")
        .count()[["pr_number"]]
        .rename(columns={"pr_number": "PRs Created"})
    )
elif interval == "Weekly":
    prs_resampled = (
        prs_df.set_index("created_at")
        .resample("W")
        .count()[["pr_number"]]
        .rename(columns={"pr_number": "PRs Created"})
    )
else:  # Monthly
    prs_resampled = (
        prs_df.set_index("created_at")
        .resample("M")
        .count()[["pr_number"]]
        .rename(columns={"pr_number": "PRs Created"})
    )

# Apply selected math function for PRs data
if math_function == "Cumulative":
    prs_final = prs_resampled.cumsum()
elif math_function == "Delta":
    prs_final = prs_resampled.diff()
elif math_function == "Percentage Change":
    prs_final = prs_resampled.pct_change() * 100
elif math_function == "Delta from the Average":
    prs_final = prs_resampled - prs_resampled.mean()
elif math_function == "Signed Log Delta":
    prs_delta = prs_resampled.diff()
    prs_final = prs_delta.apply(lambda x: np.sign(x) * np.log1p(abs(x)))
elif math_function == "Power Transform":
    prs_final = prs_resampled ** exponent
elif math_function == "Exponential Transform":
    prs_final = np.exp(prs_resampled)
elif math_function == "Log Transform":
    prs_final = np.log1p(prs_resampled)
else:
    prs_final = prs_resampled

st.subheader(f"Pull Requests Created ({interval}) ({math_function})")
st.line_chart(prs_final)

# Contributor breakdown chart for the chosen metric
st.subheader(f"Contributor Breakdown: {metric_breakdown}")
if metric_breakdown == "Commits":
    contributor_data = commits_df.groupby("author").size().reset_index(name="Count")
else:  # Lines Changed
    contributor_data = (
        commits_df.groupby("author")["total_changes"]
        .sum()
        .reset_index(name="Total Changes")
    )

value_col = contributor_data.columns[1]
contributor_data = contributor_data.sort_values(by=value_col, ascending=False)

sorted_authors = contributor_data["author"].tolist()
chart = (
    alt.Chart(contributor_data)
    .mark_bar()
    .encode(
        x=alt.X(
            "author:N",
            sort=sorted_authors,
            title="Author",
            axis=alt.Axis(labelAngle=90),
        ),
        y=alt.Y(f"{value_col}:Q", title=metric_breakdown),
    )
)

st.altair_chart(chart, use_container_width=True)
