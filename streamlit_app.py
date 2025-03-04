import streamlit as st
import pandas as pd
import altair as alt

st.title("GitHub Metrics Dashboard")


@st.cache_data
def load_data():
    commits_df = pd.read_csv("commits_data.csv", parse_dates=["date"])
    prs_df = pd.read_csv("prs_data.csv", parse_dates=["created_at", "closed_at"])
    return commits_df, prs_df


commits_df, prs_df = load_data()

# Sidebar options for time interval, contributor breakdown metric, and cumulative option
interval = st.sidebar.selectbox(
    "Select time interval", options=["Daily", "Weekly", "Byweekly", "Monthly"], index=2
)
metric_breakdown = st.sidebar.selectbox(
    "Metric for Contributor Breakdown", options=["Commits", "Lines Changed"]
)
cumulative_option = st.sidebar.checkbox("Cumulative", value=False)

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

# Apply cumulative sum if the option is selected
if cumulative_option:
    commits_final = commits_resampled.cumsum()
else:
    commits_final = commits_resampled

st.subheader(f"Commits ({interval})" + (" (Cumulative)" if cumulative_option else ""))
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

if cumulative_option:
    prs_final = prs_resampled.cumsum()
else:
    prs_final = prs_resampled

st.subheader(
    f"Pull Requests Created ({interval})"
    + (" (Cumulative)" if cumulative_option else "")
)
st.line_chart(prs_final)

# Contributor breakdown chart for the chosen metric remains as totals
st.subheader(f"Contributor Breakdown: {metric_breakdown}")
if metric_breakdown == "Commits":
    contributor_data = commits_df.groupby("author").size().reset_index(name="Count")
else:  # Lines Changed
    contributor_data = (
        commits_df.groupby("author")["total_changes"]
        .sum()
        .reset_index(name="Total Changes")
    )

# Order by descending value (highest first)
value_col = contributor_data.columns[1]
contributor_data = contributor_data.sort_values(by=value_col, ascending=False)

# Build an Altair bar chart with vertical x-axis labels
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
