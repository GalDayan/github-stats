from dotenv import load_dotenv
import requests
import datetime
import time
import pandas as pd
import os
import argparse
import logging
from urllib.parse import urlparse, parse_qs
from tqdm import tqdm  # added for progress bars

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

# Set your GitHub token and owner details here
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
GITHUB_OWNER = os.getenv('GITHUB_OWNER')
# If a single repo is set in the env, it will be used as fallback
GITHUB_REPO = os.getenv('GITHUB_REPO')
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"}

def parse_last_page(response):
    """
    Parse the 'Link' header to extract the last page number, if available.
    """
    link_header = response.headers.get("Link")
    if link_header:
        parts = link_header.split(',')
        for part in parts:
            if 'rel="last"' in part:
                start = part.find('<') + 1
                end = part.find('>')
                last_url = part[start:end]
                parsed = urlparse(last_url)
                qs = parse_qs(parsed.query)
                if "page" in qs:
                    return int(qs["page"][0])
    return None

def get_commits(owner, repo, since_date, until_date=None):
    """Fetch commits between since_date and until_date (if provided) with a progress bar."""
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    commits = []
    page = 1
    total_pages = None
    pbar = None

    while True:
        params = {"since": since_date.isoformat(), "per_page": 100, "page": page}
        if until_date:
            params["until"] = until_date.isoformat()
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            logging.error("Error fetching commits for %s/%s: %s", owner, repo, response.text)
            break
        if page == 1:
            total_pages = parse_last_page(response)
            if total_pages:
                pbar = tqdm(total=total_pages, desc=f"Fetching commits for {repo}", unit="page")
            else:
                pbar = tqdm(desc=f"Fetching commits for {repo}", unit="page")
            logging.info("Estimated total commit pages for %s: %s", repo, total_pages if total_pages else "Unknown")
        data = response.json()
        if not data:
            logging.info("No more commits found on page %s for %s.", page, repo)
            break
        commits.extend(data)
        pbar.update(1)
        page += 1
        time.sleep(1)  # pause to avoid rate limits
    pbar.close()
    logging.info("Total commits fetched for %s: %s", repo, len(commits))
    return commits

def get_commit_stats(commit_url):
    """Retrieve commit details to get the stats (additions, deletions, total)."""
    response = requests.get(commit_url, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        stats = data.get("stats", {})
        return stats
    else:
        logging.error("Error fetching commit stats: %s", response.text)
        return {}

def get_prs_between(owner, repo, start_date, end_date, state="closed"):
    """
    Fetch pull requests in the date range:
      start_date <= created_at < end_date.
    Uses a progress bar if total pages can be estimated.
    """
    prs = []
    page = 1
    total_pages = None
    pbar = None
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
    
    while True:
        params = {"state": state, "sort": "created", "direction": "desc", "per_page": 100, "page": page}
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            logging.error("Error fetching PRs for %s/%s: %s", owner, repo, response.text)
            break
        if page == 1:
            total_pages = parse_last_page(response)
            if total_pages:
                pbar = tqdm(total=total_pages, desc=f"Fetching PRs for {repo}", unit="page")
            else:
                pbar = tqdm(desc=f"Fetching PRs for {repo}", unit="page")
            logging.info("Estimated total PR pages for %s: %s", repo, total_pages if total_pages else "Unknown")
        data = response.json()
        if not data:
            logging.info("No more PRs found on page %s for %s.", page, repo)
            break
        
        for pr in data:
            # Parse the PR created_at timestamp and mark it as UTC-aware
            created_at = datetime.datetime.strptime(pr["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
            if created_at >= end_date:
                continue
            if created_at < start_date:
                logging.info("PRs older than %s reached for %s. Stopping PR fetch.", start_date, repo)
                pbar.close()
                return prs
            prs.append(pr)
        
        if total_pages:
            progress = (page / total_pages) * 100
            logging.info("Fetched PRs page %s/%s (%.2f%% complete) with %s items for %s.", page, total_pages, progress, len(data), repo)
        else:
            logging.info("Fetched PRs page %s with %s items for %s.", page, len(data), repo)
        pbar.update(1)
        page += 1
        time.sleep(1)
    pbar.close()
    logging.info("Total PRs fetched for %s: %s", repo, len(prs))
    return prs

def process_data(owner, repo, months):
    """Fetch commits and pull requests for the latest `months` period and tag the repo."""
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    since_date = now - datetime.timedelta(days=30 * months)
    
    logging.info("Starting data fetch for repository %s for the latest %s months (since %s).", repo, months, since_date)
    
    # Fetch commits and their stats
    logging.info("Fetching commits for %s...", repo)
    commits = get_commits(owner, repo, since_date)
    commit_data = []
    for commit in commits:
        commit_sha = commit["sha"]
        # commit_detail_url = commit["url"]
        # stats = get_commit_stats(commit_detail_url)
        commit_time = commit["commit"]["author"]["date"]
        author = commit["commit"]["author"].get("name", "Unknown")
        commit_data.append({
            "sha": commit_sha,
            "date": commit_time,
            "author": author,
            # "additions": stats.get("additions", 0),
            # "deletions": stats.get("deletions", 0),
            # "total_changes": stats.get("total", 0),
            "repository": repo
        })
    commits_df = pd.DataFrame(commit_data)
    if not commits_df.empty:
        commits_df["date"] = pd.to_datetime(commits_df["date"])
    logging.info("Fetched %s commits for %s.", len(commits_df), repo)
    
    # Fetch pull requests within the date range
    logging.info("Fetching pull requests for %s...", repo)
    prs = get_prs_between(owner, repo, start_date=since_date, end_date=now, state="closed")
    pr_data = []
    for pr in prs:
        pr_data.append({
            "pr_number": pr["number"],
            "created_at": pr["created_at"],
            "merged_at": pr.get("merged_at"),
            "closed_at": pr.get("closed_at"),
            "author": pr["user"]["login"],
            "repository": repo
        })
    prs_df = pd.DataFrame(pr_data)
    if not prs_df.empty:
        prs_df["created_at"] = pd.to_datetime(prs_df["created_at"])
        prs_df["closed_at"] = pd.to_datetime(prs_df["closed_at"])
    logging.info("Fetched %s pull requests for %s.", len(prs_df), repo)
    
    return commits_df, prs_df

def extend_all_commits_data(owner, repos, extend_months):
    """
    Extend the combined commits data file ("commits_data_all.csv") by fetching additional
    commits for each repository from extend_months before the earliest saved commit.
    """
    filename = "commits_data_all.csv"
    if not os.path.exists(filename):
        logging.error("%s not found. Run refetch mode first to fetch initial data.", filename)
        return None
    existing_commits_df = pd.read_csv(filename, parse_dates=["date"])
    extended_commits_list = []
    
    for repo in repos:
        repo_df = existing_commits_df[existing_commits_df["repository"] == repo]
        if repo_df.empty:
            logging.info("No existing commits for repo %s, skipping.", repo)
            continue
        earliest_date = repo_df["date"].min()
        new_since = earliest_date - datetime.timedelta(days=30 * extend_months)
        new_until = earliest_date
        logging.info("Extending commit data for repo %s: fetching commits from %s to %s.", repo, new_since, new_until)
        new_commits = get_commits(owner, repo, new_since, until_date=new_until)
        new_commit_data = []
        for commit in tqdm(new_commits, desc=f"Processing extended commits for {repo}", unit="commit"):
            commit_sha = commit["sha"]
            # commit_detail_url = commit["url"]
            # stats = get_commit_stats(commit_detail_url)
            commit_time = commit["commit"]["author"]["date"]
            author = commit["commit"]["author"].get("name", "Unknown")
            new_commit_data.append({
                "sha": commit_sha,
                "date": commit_time,
                "author": author,
                # "additions": stats.get("additions", 0),
                # "deletions": stats.get("deletions", 0),
                # "total_changes": stats.get("total", 0),
                "repository": repo
            })
        new_commits_df = pd.DataFrame(new_commit_data)
        if not new_commits_df.empty:
            new_commits_df["date"] = pd.to_datetime(new_commits_df["date"])
            extended_commits_list.append(new_commits_df)
        else:
            logging.info("No new commits found in the extended period for %s.", repo)
    
    if extended_commits_list:
        new_data = pd.concat(extended_commits_list, ignore_index=True)
        combined_df = pd.concat([existing_commits_df, new_data]).drop_duplicates(subset=["sha"]).sort_values(by="date")
        combined_df.to_csv(filename, index=False)
        logging.info("Commits data extended successfully. Total commits now: %s", len(combined_df))
        return combined_df
    else:
        logging.info("No new extended commits found for any repo.")
        return existing_commits_df

def extend_all_prs_data(owner, repos, extend_months):
    """
    Extend the combined pull requests data file ("prs_data_all.csv") by fetching additional
    PRs for each repository from extend_months before the earliest saved PR.
    """
    filename = "prs_data_all.csv"
    if not os.path.exists(filename):
        logging.error("%s not found. Run refetch mode first to fetch initial data.", filename)
        return None
    existing_prs_df = pd.read_csv(filename, parse_dates=["created_at", "closed_at"])
    extended_prs_list = []
    
    for repo in repos:
        repo_df = existing_prs_df[existing_prs_df["repository"] == repo]
        if repo_df.empty:
            logging.info("No existing PRs for repo %s, skipping.", repo)
            continue
        earliest_date = repo_df["created_at"].min()
        new_start = earliest_date - datetime.timedelta(days=30 * extend_months)
        new_end = earliest_date
        logging.info("Extending PR data for repo %s: fetching PRs from %s to %s.", repo, new_start, new_end)
        new_prs = get_prs_between(owner, repo, start_date=new_start, end_date=new_end, state="closed")
        pr_data = []
        for pr in new_prs:
            pr_data.append({
                "pr_number": pr["number"],
                "created_at": pr["created_at"],
                "merged_at": pr.get("merged_at"),
                "closed_at": pr.get("closed_at"),
                "author": pr["user"]["login"],
                "repository": repo
            })
        new_prs_df = pd.DataFrame(pr_data)
        if not new_prs_df.empty:
            new_prs_df["created_at"] = pd.to_datetime(new_prs_df["created_at"])
            new_prs_df["closed_at"] = pd.to_datetime(new_prs_df["closed_at"])
            extended_prs_list.append(new_prs_df)
        else:
            logging.info("No new PRs found in the extended period for %s.", repo)
    
    if extended_prs_list:
        new_data = pd.concat(extended_prs_list, ignore_index=True)
        combined_df = pd.concat([existing_prs_df, new_data]).drop_duplicates(subset=["pr_number"]).sort_values(by="created_at")
        combined_df.to_csv(filename, index=False)
        logging.info("PR data extended successfully. Total PRs now: %s", len(combined_df))
        return combined_df
    else:
        logging.info("No new extended PRs found for any repo.")
        return existing_prs_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="GitHub data extraction: either refetch or extend commit and PR history with progress bars."
    )
    parser.add_argument("--mode", choices=["refetch", "extend"], required=True,
                        help="Select 'refetch' to fetch recent data or 'extend' to fetch additional older data.")
    parser.add_argument("--months", type=int, default=3,
                        help="For refetch mode: fetch data for the latest X months.")
    parser.add_argument("--extend_months", type=int, default=2,
                        help="For extend mode: fetch additional data from XX months before the earliest saved record.")
    parser.add_argument("--repos", type=str, required=False,
                        help="Comma-separated list of repositories (e.g. repo1,repo2,repo3). "
                             "If not provided, the environment variable GITHUB_REPO will be used.")
    
    args = parser.parse_args()
    
    # Determine repositories to process
    if args.repos:
        repos = [repo.strip() for repo in args.repos.split(",")]
    else:
        repos = [GITHUB_REPO]
    
    if args.mode == "refetch":
        commits_list = []
        prs_list = []
        for repo in repos:
            logging.info("Running in refetch mode for repository: %s", repo)
            commits_df, prs_df = process_data(GITHUB_OWNER, repo, args.months)
            commits_list.append(commits_df)
            prs_list.append(prs_df)
        if commits_list:
            commits_all = pd.concat(commits_list, ignore_index=True).drop_duplicates(subset=["sha"]).sort_values(by="date")
            commits_all.to_csv("commits_data_all.csv", index=False)
        if prs_list:
            prs_all = pd.concat(prs_list, ignore_index=True).drop_duplicates(subset=["pr_number"]).sort_values(by="created_at")
            prs_all.to_csv("prs_data_all.csv", index=False)
        logging.info("Data extraction complete. Saved to 'commits_data_all.csv' and 'prs_data_all.csv'.")
    elif args.mode == "extend":
        logging.info("Running in extend mode for repositories: %s", repos)
        extend_all_commits_data(GITHUB_OWNER, repos, args.extend_months)
        extend_all_prs_data(GITHUB_OWNER, repos, args.extend_months)
