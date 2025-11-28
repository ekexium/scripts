# Cherry-Pick Scanner

A Python CLI tool for tracking GitHub repository cherry-pick (backport) status. This tool helps development teams ensure that PRs merged to the main branch are properly cherry-picked to release branches.

## Features

- 📊 Tracks cherry-pick status across multiple repositories
- 🎯 Monitors specific authors' PRs
- ⏰ Configurable lookback period
- 🌳 Per-repository base and target branch configuration
- 📝 Generates color-coded HTML report
- 🚨 Highlights missing and pending backports
- 💪 Fail-soft error handling (continues even if one repo fails)

## Installation

1. Clone or download this repository

2. Create a virtual environment (recommended):
```bash
python3 -m venv venv
source venv/bin/activate  # On Linux/Mac
# or
venv\Scripts\activate  # On Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

**Note**: The tool automatically generates both HTML and PDF reports. PDF generation requires system dependencies for WeasyPrint. If PDF generation fails, you'll still get the HTML report.

On Ubuntu/Debian, install system dependencies:
```bash
sudo apt-get install python3-dev libpango1.0-dev libharfbuzz-dev libffi-dev libjpeg-dev libopenjp2-7-dev
```

4. Set up GitHub authentication (choose one):

**Option 1: Using GitHub CLI (Recommended)**
```bash
gh auth login
```

**Option 2: Using environment variable**
```bash
export GITHUB_TOKEN='your_github_token_here'
```

You can create a GitHub token at: https://github.com/settings/tokens
- Required scope: `repo` (Full control of private repositories)

**Note**: The tool will automatically try `gh auth token` first, then fall back to the `GITHUB_TOKEN` environment variable.

## Configuration

Create a `config.json` file with your configuration. You can copy from `config.example.json`:

```bash
cp config.example.json config.json
```

Then edit `config.json`:

```json
{
  "github_token_env": "GITHUB_TOKEN",
  "lookback_days": 30,
  "output_file": "report.html",
  "authors": [
    "you16",
    "cfzjywxk"
  ],
  "repositories": [
    {
      "name": "pingcap/tidb",
      "base_branch": "master",
      "target_branches": [
        "release-7.5",
        "release-7.1"
      ]
    },
    {
      "name": "tikv/tikv",
      "base_branch": "master",
      "target_branches": [
        "release-7.5"
      ]
    }
  ]
}
```

### Configuration Fields

- **`github_token_env`** (optional, default: `"GITHUB_TOKEN"`): Name of environment variable containing GitHub token
- **`lookback_days`** (optional, default: `30`): Number of days to look back for merged PRs
- **`output_file`** (optional, default: `"report.html"`): Output HTML file path
- **`authors`** (required): List of GitHub usernames to track (case-insensitive)
- **`repositories`** (required): List of repository configurations
  - **`name`**: Repository in format `owner/repo`
  - **`base_branch`**: Base branch where PRs are merged (e.g., `master`, `main`)
  - **`target_branches`**: List of branches to check for backports

**Note**: GitHub usernames are case-insensitive. You can use any capitalization in the config file (e.g., `"you16"`, `"You16"`, `"YOU16"`), and the tool will match them correctly.

## Usage

**Important**: Make sure your virtual environment is activated before running:
```bash
source venv/bin/activate  # On Linux/Mac
# or
venv\Scripts\activate  # On Windows
```

Run the scanner with default config file (`config.json`):

```bash
python cherry_pick_scanner.py
```

Or specify a custom config file:

```bash
python cherry_pick_scanner.py myconfig.json
# or
python cherry_pick_scanner.py -c myconfig.json
```

Skip PDF generation (HTML only):

```bash
python cherry_pick_scanner.py --no-pdf
```

Get help:

```bash
python cherry_pick_scanner.py --help
```

Or make it executable:

```bash
chmod +x cherry_pick_scanner.py
./cherry_pick_scanner.py config.json
```

The script will:
1. Load configuration from JSON file
2. Authenticate with GitHub
3. Scan each configured repository
4. Check for backport PRs to target branches
5. Generate reports in multiple formats

**Output files**:
- `report.html` - Interactive HTML report (open in browser)
- `report.md` - Markdown report (for GitHub, documentation)
- `report.pdf` - PDF report (for printing/sharing)

All files use the filename from your config's `output_file` setting.

## Understanding the Report

### Status Indicators

- 🔴 **MISSING**: No backport PR found (needs action!)
- 🟡 **PENDING**: Backport PR is open (needs review/merge)
- 🟢 **MERGED**: Backport PR successfully merged
- ⚫ **CLOSED**: Backport PR closed without merge

### Report Sections

- **Summary Cards**: Quick overview of backport status counts
- **Main Table**: Detailed view of all PRs and their backport status
  - Sorted by priority (MISSING and PENDING first)
  - Shows PR state badges (OPEN/CLOSED/MERGED)
  - Links to original and backport PRs
  - Hover over PR titles to see full text

### What PRs Are Tracked?

The scanner tracks:
1. **Merged PRs**: PRs merged to the base branch within the lookback period
2. **Open PRs**: PRs that are still open but updated recently

This ensures you don't miss cherry-picks for PRs that are in progress or just merged.

## How It Works

1. **Scan Original PRs**: Fetches both merged and recently updated open PRs from the base branch (e.g., `master`) by specified authors

2. **Batch Search for Backports**: For each target branch, fetches all backport PRs at once and matches them locally (much faster than individual searches)
   - Matches PRs where title or body contains `#<original_pr_number>`
   - Filters by target branch

3. **Determine Status**: Categorizes each backport as MISSING, PENDING, MERGED, or CLOSED

4. **Generate Report**: Creates HTML report with color-coded status indicators

### Performance Optimizations

The scanner uses several techniques to run fast:
- **Batch fetching**: Gets all backport PRs for a branch in one API call, then matches locally
- **Early termination**: Stops scanning when PRs are too old
- **Safety limits**: Maximum 500 PRs per repository scan

For a typical scenario (4 repos, 3 target branches, 20 PRs each), the scanner makes ~16 API calls instead of ~248 (15x reduction!).

## Troubleshooting

### Authentication Error

```
ERROR: GITHUB_TOKEN environment variable is not set.
```

**Solution**: Set your GitHub token as an environment variable:
```bash
export GITHUB_TOKEN='your_token_here'
```

### Rate Limit Error

```
ERROR: Failed to authenticate with GitHub: 403
```

**Solution**: You may have hit GitHub's API rate limit. Wait a bit and try again, or use a token with higher rate limits.

### Repository Not Found

```
ERROR: Failed to access repository owner/repo: 404
```

**Solution**:
- Verify the repository name is correct (format: `owner/repo`)
- Ensure your GitHub token has access to the repository
- Check if the repository is private and requires appropriate permissions

### Branch Not Found

The script will log a warning and continue with other repositories. Verify:
- Branch name is spelled correctly
- Branch exists in the repository

## Advanced Usage

### Filtering Results

The report shows all results by default. To focus on actionable items:
- Look at the summary cards for MISSING and PENDING counts
- The table is already sorted with MISSING and PENDING items at the top

### Backport PR Matching

The tool searches for PRs using GitHub's search API with the pattern:
```
repo:owner/repo is:pr #<number> base:<target_branch>
```

This matches PRs where:
- The title or body contains `#<original_pr_number>`
- The base branch is the target branch

### Performance Considerations

- The script uses GitHub's API which has rate limits
- Scanning many repositories or long time periods may take time
- Consider adjusting `lookback_days` in your config file if scans are too slow

## Example Output

```
======================================================================
Cherry-Pick Scanner
======================================================================

Loading configuration from: config.json

Initializing GitHub client...
  Authenticated as: your_username
  Rate limit: 4999/5000

Scanning PRs merged since: 2024-10-28
Tracking authors: you16, cfzjywxk

Scanning repository: pingcap/tidb
  Fetching merged PRs from pingcap/tidb (base: master)...
    Found 15 merged PRs
  Checking PR #12345: Fix memory leak in transaction...
    → release-7.5: MERGED
    → release-7.1: PENDING
  ...

Generating HTML report...
  Report saved to: report.html

Summary:
  Total entries: 45
  Missing: 5
  Pending: 8
  Merged: 30
  Closed: 2

======================================================================
Scan complete!
======================================================================
```

## Contributing

Feel free to submit issues or pull requests for improvements.

## License

This tool is provided as-is for internal development team use.
