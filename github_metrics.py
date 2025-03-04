from dotenv import load_dotenv
import requests
import datetime
import time
import pandas as pd
import os

load_dotenv()

# Set your GitHub token here
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
GITHUB_OWNER = os.getenv('GITHUB_OWNER')
GITHUB_REPO =  os.getenv('GITHUB_REPO')
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"}

def get_commits(owner, repo, since_date):
    """Fetch commits since a given date."""
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    commits = []
    page = 1
    while True:
        params = {"since": since_date.isoformat(), "per_page": 100, "page": page}
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print("Error fetching commits:", response.text)
            break
        data = response.json()
        if not data:
            break
        commits.extend(data)
        page += 1
        time.sleep(1)  # pause to avoid rate limits
    return commits

def get_commit_stats(commit_url):
    """Retrieve the commit details to get the stats (additions, deletions, total)."""
    response = requests.get(commit_url, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        stats = data.get("stats", {})
        return stats
    else:
        return {}

def get_pull_requests(owner, repo, since_date, state="closed"):
    """
    Fetch pull requests (here we fetch closed ones) and stop once we reach PRs older than our date.
    """
    prs = []
    page = 1
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
    while True:
        params = {"state": state, "sort": "created", "direction": "desc", "per_page": 100, "page": page}
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print("Error fetching PRs:", response.text)
            break
        data = response.json()
        if not data:
            break
        for pr in data:
            # Convert created_at to datetime object
            created_at = datetime.datetime.strptime(pr["created_at"], "%Y-%m-%dT%H:%M:%SZ")
            if created_at < since_date:
                return prs  # stop once PRs are older than our cutoff
            prs.append(pr)
        page += 1
        time.sleep(1)
    return prs

def process_data(owner, repo, months):
    now = datetime.datetime.utcnow()
    since_date = now - datetime.timedelta(days=30 * months)
    
    # Get commits and their stats
    print("Fetching commits...")
    commits = get_commits(owner, repo, since_date)
    commit_data = []
    for commit in commits:
        commit_sha = commit["sha"]
        commit_detail_url = commit["url"]
        stats = get_commit_stats(commit_detail_url)
        commit_time = commit["commit"]["author"]["date"]
        # Use the commit author name; fallback to "Unknown" if necessary.
        author = commit["commit"]["author"].get("name", "Unknown")
        commit_data.append({
            "sha": commit_sha,
            "date": commit_time,
            "author": author,
            "additions": stats.get("additions", 0),
            "deletions": stats.get("deletions", 0),
            "total_changes": stats.get("total", 0)
        })
    
    commits_df = pd.DataFrame(commit_data)
    commits_df["date"] = pd.to_datetime(commits_df["date"])
    
    # Get PRs created (using closed PRs here for created/resolved metrics)
    print("Fetching pull requests created/resolved...")
    prs = get_pull_requests(owner, repo, since_date, state="closed")
    pr_data = []
    for pr in prs:
        pr_data.append({
            "pr_number": pr["number"],
            "created_at": pr["created_at"],
            "merged_at": pr.get("merged_at"),
            "closed_at": pr.get("closed_at"),
            "author": pr["user"]["login"]
        })
    prs_df = pd.DataFrame(pr_data)
    if not prs_df.empty:
        prs_df["created_at"] = pd.to_datetime(prs_df["created_at"])
        prs_df["closed_at"] = pd.to_datetime(prs_df["closed_at"])
    
    return commits_df, prs_df

if __name__ == "__main__":
    # Customize these variables as needed
    months = 3  # Adjust to pull data for the latest X months
    
    commits_df, prs_df = process_data(GITHUB_OWNER, GITHUB_REPO, months)
    
    # Save data to CSV files for the visualization UI
    commits_df.to_csv("commits_data.csv", index=False)
    prs_df.to_csv("prs_data.csv", index=False)
    print("Data extraction complete. Saved to 'commits_data.csv' and 'prs_data.csv'.")
