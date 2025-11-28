#!/usr/bin/env python3
"""
Cherry-Pick Scanner
A tool to track GitHub repository cherry-pick (backport) status.
"""

import os
import sys
import json
import argparse
import subprocess
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any
from github import Github, GithubException
from jinja2 import Template

# ============================================================================
# CONFIGURATION
# ============================================================================

def get_github_token(github_token_env: str) -> Optional[str]:
    """
    Get GitHub token, trying gh CLI first, then environment variable.

    Args:
        github_token_env: Name of environment variable to check

    Returns:
        GitHub token string or None
    """
    # First, try to get token from gh CLI
    try:
        result = subprocess.run(
            ['gh', 'auth', 'token'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            token = result.stdout.strip()
            if token:
                print("  Using token from gh CLI")
                return token
    except (subprocess.TimeoutExpired, FileNotFoundError):
        # gh CLI not available or timed out
        pass

    # Fall back to environment variable
    token = os.getenv(github_token_env)
    if token:
        print(f"  Using token from environment variable: {github_token_env}")
        return token

    return None


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from JSON file.

    Args:
        config_path: Path to configuration JSON file

    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        # Validate required fields
        required_fields = ['repositories', 'authors']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field in config: {field}")

        # Set defaults for optional fields
        config.setdefault('github_token_env', 'GITHUB_TOKEN')
        config.setdefault('lookback_days', 30)
        config.setdefault('output_file', 'report.html')

        return config

    except FileNotFoundError:
        print(f"ERROR: Configuration file not found: {config_path}")
        print("Please create a config file. See config.example.json for reference.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in configuration file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to load configuration: {e}")
        sys.exit(1)

# ============================================================================
# CORE LOGIC
# ============================================================================

def get_relevant_prs(repo, base_branch: str, authors: List[str], since_date: datetime) -> List[Any]:
    """
    Fetch relevant PRs from a repository (both merged and recently updated open PRs).

    Args:
        repo: PyGithub repository object
        base_branch: Base branch to filter PRs
        authors: List of author usernames to filter (case-insensitive)
        since_date: Only include PRs updated after this date

    Returns:
        List of PR objects
    """
    print(f"  Fetching PRs from {repo.full_name} (base: {base_branch})...")

    # Normalize author names to lowercase for case-insensitive comparison
    authors_lower = [author.lower() for author in authors]

    relevant_prs = []
    try:
        # Strategy: Fetch PRs sorted by update time, process both open and closed
        # This is more efficient than separate queries
        pulls = repo.get_pulls(
            state='all',  # Get both open and closed
            base=base_branch,
            sort='updated',
            direction='desc'
        )

        # Filter for relevant PRs by specified authors within the date range
        count = 0
        for pr in pulls:
            # Stop if we've gone too far back in time
            if pr.updated_at < since_date:
                break

            # GitHub usernames are case-insensitive, so compare in lowercase
            if pr.user and pr.user.login.lower() in authors_lower:
                # Include if:
                # 1. Merged recently, OR
                # 2. Open and updated recently
                if pr.state == 'open' or (pr.merged and pr.merged_at >= since_date):
                    relevant_prs.append(pr)

            count += 1
            # Safety limit to avoid scanning too many PRs
            if count >= 500:
                print(f"    (Reached safety limit of 500 PRs)")
                break

        print(f"    Found {len(relevant_prs)} relevant PRs (scanned {count} total)")

    except GithubException as e:
        print(f"    ERROR: Failed to fetch PRs: {e}")

    return relevant_prs


def find_backport_prs_batch(g: Github, repo, original_prs: List[Any], target_branch: str) -> Dict[int, Any]:
    """
    Search for backport PRs in batch (more efficient than individual searches).
    Also checks if commits are already in target branch (rebase case).

    Args:
        g: Github client instance
        repo: PyGithub repository object
        original_prs: List of original PR objects
        target_branch: Target branch for the backports

    Returns:
        Dictionary mapping original PR number to:
        - Backport PR object, OR
        - 'ALREADY_IN_BRANCH' string if commit is already in target branch, OR
        - None if missing
    """
    result = {}

    # First, get all PRs for this target branch at once
    try:
        print(f"    Fetching all backport PRs for {target_branch}...")
        backport_prs = list(repo.get_pulls(
            state='all',
            base=target_branch,
            sort='updated',
            direction='desc'
        ))

        print(f"    Found {len(backport_prs)} total PRs in {target_branch}")

        # Build a mapping of referenced PR numbers to backport PRs
        import re
        pr_number_pattern = re.compile(r'#(\d+)')

        backport_map = {}  # original_pr_number -> [list of backport PRs]
        for backport_pr in backport_prs:
            # Look for PR numbers in title and body
            text = backport_pr.title + ' ' + (backport_pr.body or '')
            referenced_numbers = set(int(num) for num in pr_number_pattern.findall(text))

            for num in referenced_numbers:
                if num not in backport_map:
                    backport_map[num] = []
                backport_map[num].append(backport_pr)

        # Now match original PRs to their backports
        print(f"    Checking if commits are already in {target_branch}...")
        for original_pr in original_prs:
            if original_pr.number in backport_map:
                candidates = backport_map[original_pr.number]
                # If multiple matches, pick the most recent
                if len(candidates) > 1:
                    candidates.sort(key=lambda x: x.created_at, reverse=True)
                result[original_pr.number] = candidates[0]
            else:
                # Check if commit is already in target branch (rebase case)
                # Only check for merged PRs
                if original_pr.merged and original_pr.merge_commit_sha:
                    try:
                        # Check if the merge commit exists in target branch
                        compare = repo.compare(target_branch, original_pr.merge_commit_sha)
                        # If behind_by is 0, the commit is already in target branch
                        if compare.behind_by == 0:
                            result[original_pr.number] = 'ALREADY_IN_BRANCH'
                        else:
                            result[original_pr.number] = None
                    except:
                        # If comparison fails, assume not in branch
                        result[original_pr.number] = None
                else:
                    result[original_pr.number] = None

    except GithubException as e:
        print(f"    WARNING: Error fetching backport PRs: {e}")
        # Fall back to individual searches
        for original_pr in original_prs:
            result[original_pr.number] = find_backport_pr_single(g, repo, original_pr.number, target_branch)

    return result


def find_backport_pr_single(g: Github, repo, original_pr_number: int, target_branch: str) -> Optional[Any]:
    """
    Search for a single backport PR (fallback for batch search).

    Args:
        g: Github client instance
        repo: PyGithub repository object
        original_pr_number: Original PR number to search for
        target_branch: Target branch for the backport

    Returns:
        PR object if found, None otherwise
    """
    try:
        search_query = f"repo:{repo.full_name} is:pr #{original_pr_number} base:{target_branch}"
        results = g.search_issues(search_query)
        matching_prs = list(results)

        if not matching_prs:
            return None

        if len(matching_prs) > 1:
            matching_prs.sort(key=lambda x: x.created_at, reverse=True)

        backport_pr = matching_prs[0]
        pr_pattern = f"#{original_pr_number}"

        if pr_pattern in backport_pr.title or (backport_pr.body and pr_pattern in backport_pr.body):
            return backport_pr

        return None

    except GithubException as e:
        print(f"    WARNING: Error searching for backport PR #{original_pr_number}: {e}")
        return None


def determine_status(backport_pr: Any) -> str:
    """
    Determine the status of a backport based on the PR state or special markers.

    Args:
        backport_pr: PR object, 'ALREADY_IN_BRANCH' string, or None

    Returns:
        Status string: IN_BRANCH, MISSING, PENDING, MERGED, or CLOSED
    """
    # Special case: commit already in target branch (rebase case)
    if backport_pr == 'ALREADY_IN_BRANCH':
        return 'IN_BRANCH'

    if backport_pr is None:
        return 'MISSING'

    if backport_pr.state == 'open':
        return 'PENDING'

    # For closed PRs, check if merged
    # Note: PyGithub Issue objects (from search) may not have 'merged' attribute
    # We need to fetch the actual PR object
    try:
        if hasattr(backport_pr, 'pull_request') and backport_pr.pull_request:
            # This is an issue object from search, need to check if it was merged
            # Check for merged label or state
            if any(label.name.lower() == 'merged' for label in backport_pr.labels):
                return 'MERGED'
            # If closed without merged label, likely closed without merge
            return 'CLOSED'

        # If we have the merged attribute directly
        if hasattr(backport_pr, 'merged'):
            return 'MERGED' if backport_pr.merged else 'CLOSED'

        # Fallback: check state_reason or labels
        if backport_pr.state == 'closed':
            return 'CLOSED'

    except Exception:
        pass

    return 'CLOSED'


def scan_repository(g: Github, repo_config: Dict[str, Any], authors: List[str], since_date: datetime) -> List[Dict]:
    """
    Scan a single repository for cherry-pick status.

    Args:
        g: Github client instance
        repo_config: Repository configuration dict
        authors: List of author usernames
        since_date: Date threshold for PRs

    Returns:
        List of result dictionaries
    """
    repo_name = repo_config['name']
    base_branch = repo_config['base_branch']
    target_branches = repo_config['target_branches']

    print(f"\nScanning repository: {repo_name}")

    results = []

    try:
        repo = g.get_repo(repo_name)

        # Get relevant PRs from base branch (merged + recently updated open PRs)
        relevant_prs = get_relevant_prs(repo, base_branch, authors, since_date)

        if not relevant_prs:
            print(f"  No relevant PRs found in the specified time range")
            return results

        # For each target branch, batch search all backports at once
        for target_branch in target_branches:
            print(f"\n  Checking backports to {target_branch}...")

            # Batch search for all backport PRs for this target branch
            backport_map = find_backport_prs_batch(g, repo, relevant_prs, target_branch)

            # Build results for each original PR
            for pr in relevant_prs:
                backport_pr = backport_map.get(pr.number)
                status = determine_status(backport_pr)

                # Determine the date to display
                pr_date = pr.merged_at if pr.merged else pr.updated_at

                # Build result entry
                result = {
                    'author': pr.user.login,
                    'repo': repo_name,
                    'original_pr': {
                        'number': pr.number,
                        'title': pr.title,
                        'url': pr.html_url,
                        'state': pr.state,
                        'date': pr_date
                    },
                    'target_branch': target_branch,
                    'status': status,
                    'backport_pr': None
                }

                # Add backport PR info if exists (and is not special marker)
                if backport_pr and backport_pr != 'ALREADY_IN_BRANCH':
                    result['backport_pr'] = {
                        'number': backport_pr.number,
                        'title': backport_pr.title,
                        'url': backport_pr.html_url,
                        'state': backport_pr.state
                    }

                results.append(result)

            # Print summary for this target branch
            status_counts = {}
            for r in results:
                if r['target_branch'] == target_branch:
                    status_counts[r['status']] = status_counts.get(r['status'], 0) + 1

            print(f"    Summary: {dict(status_counts)}")

    except GithubException as e:
        print(f"  ERROR: Failed to access repository {repo_name}: {e}")
    except Exception as e:
        print(f"  ERROR: Unexpected error scanning {repo_name}: {e}")

    return results


# ============================================================================
# REPORT GENERATION
# ============================================================================

def generate_markdown_report(results: List[Dict], output_path: str, lookback_days: int, authors: List[str], repo_count: int, stats: Dict[str, int]):
    """
    Generate Markdown report from scan results.

    Args:
        results: List of result dictionaries (already sorted)
        output_path: Path to output Markdown file
        lookback_days: Number of days scanned
        authors: List of authors tracked
        repo_count: Number of repositories scanned
        stats: Statistics dictionary
    """
    md_lines = []

    # Header
    md_lines.append("# Cherry-Pick Status Report")
    md_lines.append("")
    md_lines.append(f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    md_lines.append(f"**Lookback Period:** {lookback_days} days")
    md_lines.append(f"**Repositories:** {repo_count}")
    md_lines.append(f"**Authors:** {', '.join(authors)}")
    md_lines.append("")

    # Summary
    md_lines.append("## Summary")
    md_lines.append("")
    md_lines.append(f"| Total | Missing | Pending | Merged | In Branch | Closed |")
    md_lines.append(f"|-------|---------|---------|--------|-----------|--------|")
    md_lines.append(f"| {stats['total']} | {stats['missing']} | {stats['pending']} | {stats['merged']} | {stats['in_branch']} | {stats['closed']} |")
    md_lines.append("")

    if not results:
        md_lines.append("**✓ All Clear!** No backport tracking needed for the specified time period.")
    else:
        # Main table
        md_lines.append("## Backport Status")
        md_lines.append("")
        md_lines.append("| Author | Repository | Original PR | Target Branch | Status | Backport PR |")
        md_lines.append("|--------|------------|-------------|---------------|--------|-------------|")

        for result in results:
            author = result['author']
            repo = result['repo']

            # Original PR - simple format without line breaks
            orig_pr_num = result['original_pr']['number']
            orig_pr_url = result['original_pr']['url']
            orig_pr_title = result['original_pr']['title'].replace('|', '\\|')  # Escape pipes
            orig_pr_state = result['original_pr']['state'].upper()

            # Truncate title if too long
            max_title_len = 50
            if len(orig_pr_title) > max_title_len:
                orig_pr_title = orig_pr_title[:max_title_len] + '...'

            orig_pr = f"[#{orig_pr_num}]({orig_pr_url}) `{orig_pr_state}` {orig_pr_title}"

            # Target branch
            target = f"`{result['target_branch']}`"

            # Status with emoji
            status_emoji = {
                'MISSING': '🔴',
                'PENDING': '🟡',
                'MERGED': '🟢',
                'IN_BRANCH': '🔵',
                'CLOSED': '⚫'
            }
            status = f"{status_emoji.get(result['status'], '')} {result['status']}"

            # Backport PR - simple format without line breaks
            if result['backport_pr']:
                bp_num = result['backport_pr']['number']
                bp_url = result['backport_pr']['url']
                bp_title = result['backport_pr']['title'].replace('|', '\\|')
                bp_state = result['backport_pr']['state'].upper()

                # Truncate title if too long
                if len(bp_title) > max_title_len:
                    bp_title = bp_title[:max_title_len] + '...'

                backport = f"[#{bp_num}]({bp_url}) `{bp_state}` {bp_title}"
            else:
                backport = "—"

            md_lines.append(f"| {author} | {repo} | {orig_pr} | {target} | {status} | {backport} |")

        md_lines.append("")

        # Legend
        md_lines.append("## Legend")
        md_lines.append("")
        md_lines.append("- 🔴 **MISSING**: No backport PR found (needs action!)")
        md_lines.append("- 🟡 **PENDING**: Backport PR is open (needs review/merge)")
        md_lines.append("- 🟢 **MERGED**: Backport PR successfully merged")
        md_lines.append("- 🔵 **IN_BRANCH**: Commit already in target branch (rebased)")
        md_lines.append("- ⚫ **CLOSED**: Backport PR closed without merge")

    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(md_lines))


# ============================================================================
# HTML REPORT GENERATION
# ============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cherry-Pick Status Report</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f5f5f5;
            padding: 20px;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        h1 {
            color: #2c3e50;
            margin-bottom: 10px;
            font-size: 28px;
        }

        .metadata {
            color: #7f8c8d;
            margin-bottom: 30px;
            font-size: 14px;
        }

        .summary {
            display: flex;
            gap: 20px;
            margin-bottom: 30px;
            flex-wrap: wrap;
        }

        .summary-card {
            flex: 1;
            min-width: 150px;
            padding: 15px;
            border-radius: 6px;
            background-color: #f8f9fa;
            border-left: 4px solid #dee2e6;
        }

        .summary-card.missing {
            border-left-color: #dc3545;
            background-color: #fff5f5;
        }

        .summary-card.pending {
            border-left-color: #ffc107;
            background-color: #fffef5;
        }

        .summary-card.merged {
            border-left-color: #28a745;
            background-color: #f5fff5;
        }

        .summary-card .label {
            font-size: 12px;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .summary-card .value {
            font-size: 32px;
            font-weight: bold;
            color: #2c3e50;
            margin-top: 5px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            font-size: 12px;
            table-layout: fixed;
        }

        thead {
            background-color: #2c3e50;
            color: white;
        }

        /* Print-specific styles for PDF */
        @media print {
            body {
                background-color: white;
                padding: 0;
            }

            .container {
                max-width: 100%;
                padding: 10px;
                box-shadow: none;
            }

            h1 {
                font-size: 20px;
                margin-bottom: 5px;
            }

            .metadata {
                font-size: 10px;
                margin-bottom: 15px;
            }

            .summary {
                gap: 10px;
                margin-bottom: 15px;
            }

            .summary-card {
                min-width: 100px;
                padding: 8px;
            }

            .summary-card .label {
                font-size: 9px;
            }

            .summary-card .value {
                font-size: 18px;
            }

            table {
                font-size: 9px;
                page-break-inside: auto;
            }

            tr {
                page-break-inside: avoid;
                page-break-after: auto;
            }

            thead {
                display: table-header-group;
            }

            th, td {
                padding: 6px 4px;
            }

            .pr-title {
                max-width: none;
                font-size: 8px;
                word-wrap: break-word;
                white-space: normal;
            }

            .pr-state {
                font-size: 7px;
                padding: 1px 4px;
            }

            .status-badge {
                font-size: 8px;
                padding: 2px 6px;
            }

            a {
                color: #007bff;
                text-decoration: none;
            }

            /* Column widths for print */
            th:nth-child(1), td:nth-child(1) { width: 10%; } /* Author */
            th:nth-child(2), td:nth-child(2) { width: 15%; } /* Repository */
            th:nth-child(3), td:nth-child(3) { width: 30%; } /* Original PR */
            th:nth-child(4), td:nth-child(4) { width: 15%; } /* Target Branch */
            th:nth-child(5), td:nth-child(5) { width: 10%; } /* Status */
            th:nth-child(6), td:nth-child(6) { width: 20%; } /* Backport PR */
        }

        @page {
            size: A4 landscape;
            margin: 1cm;
        }

        th {
            padding: 12px;
            text-align: left;
            font-weight: 600;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        td {
            padding: 12px;
            border-bottom: 1px solid #dee2e6;
        }

        tbody tr:hover {
            background-color: #f8f9fa;
        }

        .status-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .status-missing {
            background-color: #dc3545;
            color: white;
        }

        .status-pending {
            background-color: #ffc107;
            color: #333;
        }

        .status-merged {
            background-color: #28a745;
            color: white;
        }

        .status-in_branch {
            background-color: #17a2b8;
            color: white;
        }

        .status-closed {
            background-color: #6c757d;
            color: white;
        }

        a {
            color: #007bff;
            text-decoration: none;
        }

        a:hover {
            text-decoration: underline;
        }

        .pr-title {
            max-width: 400px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .pr-state {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
            margin-left: 5px;
        }

        .pr-state-open {
            background-color: #28a745;
            color: white;
        }

        .pr-state-closed {
            background-color: #6f42c1;
            color: white;
        }

        .pr-state-merged {
            background-color: #6f42c1;
            color: white;
        }

        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #6c757d;
        }

        .empty-state h2 {
            color: #28a745;
            margin-bottom: 10px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Cherry-Pick Status Report</h1>
        <div class="metadata">
            Generated: {{ generation_time }}<br>
            Lookback Period: {{ lookback_days }} days<br>
            Repositories: {{ repo_count }}<br>
            Authors: {{ authors|join(', ') }}
        </div>

        {% if results %}
        <div class="summary">
            <div class="summary-card">
                <div class="label">Total</div>
                <div class="value">{{ stats.total }}</div>
            </div>
            <div class="summary-card missing">
                <div class="label">Missing</div>
                <div class="value">{{ stats.missing }}</div>
            </div>
            <div class="summary-card pending">
                <div class="label">Pending</div>
                <div class="value">{{ stats.pending }}</div>
            </div>
            <div class="summary-card merged">
                <div class="label">Merged</div>
                <div class="value">{{ stats.merged }}</div>
            </div>
            <div class="summary-card">
                <div class="label">In Branch</div>
                <div class="value">{{ stats.in_branch }}</div>
            </div>
            <div class="summary-card">
                <div class="label">Closed</div>
                <div class="value">{{ stats.closed }}</div>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th>Author</th>
                    <th>Repository</th>
                    <th>Original PR</th>
                    <th>Target Branch</th>
                    <th>Status</th>
                    <th>Backport PR</th>
                </tr>
            </thead>
            <tbody>
                {% for result in results %}
                <tr>
                    <td>{{ result.author }}</td>
                    <td>{{ result.repo }}</td>
                    <td>
                        <a href="{{ result.original_pr.url }}" target="_blank">
                            #{{ result.original_pr.number }}
                        </a>
                        <span class="pr-state pr-state-{{ result.original_pr.state }}">{{ result.original_pr.state|upper }}</span>
                        <div class="pr-title" title="{{ result.original_pr.title }}">
                            {{ result.original_pr.title }}
                        </div>
                    </td>
                    <td><code>{{ result.target_branch }}</code></td>
                    <td>
                        <span class="status-badge status-{{ result.status|lower }}">
                            {{ result.status }}
                        </span>
                    </td>
                    <td>
                        {% if result.backport_pr %}
                        <a href="{{ result.backport_pr.url }}" target="_blank">
                            #{{ result.backport_pr.number }}
                        </a>
                        <span class="pr-state pr-state-{{ result.backport_pr.state }}">{{ result.backport_pr.state|upper }}</span>
                        <div class="pr-title" title="{{ result.backport_pr.title }}">
                            {{ result.backport_pr.title }}
                        </div>
                        {% else %}
                        <span style="color: #6c757d;">—</span>
                        {% endif %}
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% else %}
        <div class="empty-state">
            <h2>✓ All Clear!</h2>
            <p>No backport tracking needed for the specified time period.</p>
        </div>
        {% endif %}
    </div>
</body>
</html>
"""


def generate_html_report(results: List[Dict], output_path: str, lookback_days: int, authors: List[str], repo_count: int, generate_pdf: bool = True):
    """
    Generate HTML report from scan results.

    Args:
        results: List of result dictionaries
        output_path: Path to output HTML file
        lookback_days: Number of days scanned
        authors: List of authors tracked
        repo_count: Number of repositories scanned
    """
    print(f"\nGenerating HTML report...")

    # Sort results by priority: MISSING, PENDING, CLOSED, MERGED, IN_BRANCH
    priority_order = {'MISSING': 0, 'PENDING': 1, 'CLOSED': 2, 'MERGED': 3, 'IN_BRANCH': 4}
    sorted_results = sorted(results, key=lambda x: (
        priority_order.get(x['status'], 5),
        x['repo'],
        x['original_pr']['number']
    ))

    # Calculate statistics
    stats = {
        'total': len(results),
        'missing': sum(1 for r in results if r['status'] == 'MISSING'),
        'pending': sum(1 for r in results if r['status'] == 'PENDING'),
        'merged': sum(1 for r in results if r['status'] == 'MERGED'),
        'in_branch': sum(1 for r in results if r['status'] == 'IN_BRANCH'),
        'closed': sum(1 for r in results if r['status'] == 'CLOSED')
    }

    # Render template
    template = Template(HTML_TEMPLATE)
    html_content = template.render(
        generation_time=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
        lookback_days=lookback_days,
        authors=authors,
        repo_count=repo_count,
        results=sorted_results,
        stats=stats
    )

    # Write HTML to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"  HTML report saved to: {output_path}")

    # Generate Markdown report
    md_path = output_path.replace('.html', '.md')
    if md_path != output_path:  # Only if output was HTML
        try:
            print(f"  Generating Markdown report...")
            generate_markdown_report(sorted_results, md_path, lookback_days, authors, repo_count, stats)
            print(f"  Markdown report saved to: {md_path}")
        except Exception as e:
            print(f"  WARNING: Failed to generate Markdown: {e}")

    # Generate PDF if requested
    if generate_pdf:
        pdf_path = output_path.replace('.html', '.pdf')
        if pdf_path != output_path:  # Only if output was HTML
            try:
                from weasyprint import HTML
                print(f"  Generating PDF report...")
                HTML(string=html_content).write_pdf(pdf_path)
                print(f"  PDF report saved to: {pdf_path}")
            except ImportError:
                print(f"  (Skipping PDF generation - weasyprint not installed)")
            except Exception as e:
                print(f"  WARNING: Failed to generate PDF: {e}")

    print(f"\nSummary:")
    print(f"  Total entries: {stats['total']}")
    print(f"  Missing: {stats['missing']}")
    print(f"  Pending: {stats['pending']}")
    print(f"  Merged: {stats['merged']}")
    print(f"  In Branch: {stats['in_branch']}")
    print(f"  Closed: {stats['closed']}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Track GitHub repository cherry-pick (backport) status',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s config.json
  %(prog)s --config myconfig.json
  %(prog)s -c config.json

Configuration file format (JSON):
  {
    "github_token_env": "GITHUB_TOKEN",
    "lookback_days": 30,
    "output_file": "report.html",
    "authors": ["username1", "username2"],
    "repositories": [
      {
        "name": "owner/repo",
        "base_branch": "master",
        "target_branches": ["release-7.5"]
      }
    ]
  }
        """
    )
    parser.add_argument(
        'config',
        nargs='?',
        default='config.json',
        help='Path to configuration JSON file (default: config.json)'
    )
    parser.add_argument(
        '-c', '--config',
        dest='config_alt',
        help='Alternative way to specify config file path'
    )
    parser.add_argument(
        '--no-pdf',
        action='store_true',
        help='Skip PDF generation (only generate HTML)'
    )

    args = parser.parse_args()

    # Determine config file path
    config_path = args.config_alt if args.config_alt else args.config

    print("=" * 70)
    print("Cherry-Pick Scanner")
    print("=" * 70)

    # Load configuration
    print(f"\nLoading configuration from: {config_path}")
    config = load_config(config_path)

    # Extract configuration values
    github_token_env = config['github_token_env']
    lookback_days = config['lookback_days']
    output_file = config['output_file']
    authors = config['authors']
    repositories = config['repositories']

    # Get GitHub token (try gh CLI first, then environment variable)
    print(f"\nObtaining GitHub token...")
    github_token = get_github_token(github_token_env)
    if not github_token:
        print(f"\nERROR: Could not obtain GitHub token.")
        print("Please either:")
        print("  1. Authenticate with GitHub CLI: gh auth login")
        print(f"  2. Set environment variable: export {github_token_env}='your_token_here'")
        sys.exit(1)

    # Validate configuration
    if not repositories:
        print("\nERROR: No repositories configured. Please add at least one repository.")
        sys.exit(1)

    if not authors:
        print("\nERROR: No authors configured. Please add at least one author.")
        sys.exit(1)

    # Initialize GitHub client
    print(f"\nInitializing GitHub client...")
    try:
        from github import Auth
        auth = Auth.Token(github_token)
        g = Github(auth=auth)

        # Test authentication
        user = g.get_user()
        print(f"  Authenticated as: {user.login}")

        # Check rate limit
        try:
            rate_limit = g.get_rate_limit()
            print(f"  Rate limit: {rate_limit.resources.core.remaining}/{rate_limit.resources.core.limit}")
        except (AttributeError, Exception):
            # Fallback if rate limit API is unavailable
            print(f"  (Rate limit info unavailable)")

    except GithubException as e:
        print(f"\nERROR: Failed to authenticate with GitHub: {e}")
        sys.exit(1)

    # Calculate date threshold
    since_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    print(f"\nScanning PRs merged since: {since_date.strftime('%Y-%m-%d')}")
    print(f"Tracking authors: {', '.join(authors)}")

    # Scan all repositories
    all_results = []
    for repo_config in repositories:
        results = scan_repository(g, repo_config, authors, since_date)
        all_results.extend(results)

    # Generate HTML report
    generate_html_report(
        results=all_results,
        output_path=output_file,
        lookback_days=lookback_days,
        authors=authors,
        repo_count=len(repositories),
        generate_pdf=not args.no_pdf
    )

    print("\n" + "=" * 70)
    print("Scan complete!")
    print("=" * 70)


if __name__ == '__main__':
    main()
