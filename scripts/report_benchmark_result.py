import os
import json
import requests
import re
import sys
from datetime import datetime
from upload_benchmark_assets import publish_result_assets
from generate_memory_charts import generate_memory_comparison_chart
from generate_performance_charts import generate_read_write_time_chart
from append_to_history import append_benchmark_to_sqlite
from generate_trend_charts import generate_performance_trend_from_sqlite

# ------------------- Configuration -------------------
# Calculate paths relative to project root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = "result"
RESULT_DIR = os.path.join(REPO_ROOT, BASE_DIR)
COMMENT_MD = os.path.join(RESULT_DIR, "benchmark_comment.md")

# Flat layout (Docker output): result/results_java.json, result/results_parquet_java.json, result/memory_usage_*.csv
TSFILE_JSON = ["results_java.json", "results_python.json", "results_cpp.json"]
PARQUET_JSON = ["results_parquet_java.json", "results_parquet_python.json", "results_parquet_cpp.json"]

# GitHub settings (fill in before use)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
# Asset repository (for archiving and uploading benchmark results)
ASSET_REPO = os.getenv("ASSET_REPO", "ColinLeeo/TsFile-BenchMark").strip()
ASSET_COMMIT_MESSAGE = f"chore(benchmark): publish benchmark assets on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
# Issue repository (for posting benchmark results)
ISSUE_REPO = os.getenv("ISSUE_REPO", "ColinLeeo/tsfile").strip()
ISSUE_NUMBER = int(os.getenv("ISSUE_NUMBER", "2"))
REQUEST_TIMEOUT = 15
# -----------------------------------------------------

METRIC_KEYS_TO_ROUND = ["reading_time", "writing_time", "reading_speed", "writing_speed"]

def _first_value(data, keys):
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return None


def normalize_result_metrics_to_two_decimals():
    """Normalize read/write time and speed values in result JSON files to two decimals."""
    result_files = TSFILE_JSON + PARQUET_JSON
    for filename in result_files:
        file_path = os.path.join(RESULT_DIR, filename)
        if not os.path.exists(file_path):
            continue

        with open(file_path, "r") as f:
            data = json.load(f)

        changed = False

        for key in METRIC_KEYS_TO_ROUND:
            value = data.get(key)
            if isinstance(value, (int, float)):
                rounded = round(float(value), 2)
                if rounded != value:
                    data[key] = rounded
                    changed = True

        if changed:
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)


def load_flat_result(json_filename, format_label, lang_label):
    """Load one benchmark JSON from RESULT_DIR (flat). Returns row or None if missing."""
    path = os.path.join(RESULT_DIR, json_filename)
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        data = json.load(f)

    read_time_sec = _first_value(data, ["reading_time"])
    write_time_sec = _first_value(data, ["writing_time"])
    read_speed = _first_value(data, ["reading_speed"])
    write_speed = _first_value(data, ["writing_speed"])
    file_size_kb = _first_value(data, ["tsfile_size", "file_size_kb", "file_size"])

    read_ms = round(read_time_sec * 1000, 2) if isinstance(read_time_sec, (int, float)) else ""
    write_ms = round(write_time_sec * 1000, 2) if isinstance(write_time_sec, (int, float)) else ""
    size_mb = round(file_size_kb / 1024, 2) if isinstance(file_size_kb, (int, float)) else ""

    return [format_label, lang_label, read_ms, read_speed, write_ms, write_speed, size_mb]


# Step 1: Parse benchmark data (TsFile + Parquet, flat layout)
normalize_result_metrics_to_two_decimals()

benchmark_data = []
for fname in TSFILE_JSON:
    if "java" in fname:
        lang_label = "Java"
    elif "python" in fname:
        lang_label = "Python"
    else:
        lang_label = "C++"
    row = load_flat_result(fname, "TsFile", lang_label)
    if row:
        benchmark_data.append(row)
for fname in PARQUET_JSON:
    if "java" in fname:
        lang_label = "Java"
    elif "python" in fname:
        lang_label = "Python"
    else:
        lang_label = "C++"
    row = load_flat_result(fname, "Parquet", lang_label)
    if row:
        benchmark_data.append(row)

if not benchmark_data:
    raise FileNotFoundError(
        f"No benchmark JSON found under {RESULT_DIR}. "
        f"Expected e.g. {TSFILE_JSON}, {PARQUET_JSON}"
    )

# Step 2: Generate markdown table
table_header = "| Format | Language | Read Time (s) | Read Speed (points/s) | Write Time (s) | Write Speed (points/s) | File Size (MB) |\n"
table_divider = "|--------|---------|--------------|---------------------|---------------|---------------------|---------------|\n"
table_rows = [
    f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} |"
    for row in benchmark_data
]

# Step 2.5: Generate memory usage comparison chart
try:
    generate_read_write_time_chart(benchmark_data, RESULT_DIR)
except Exception as e:
    print(f"Read/write time chart generation failed: {e}")

try:
    generate_memory_comparison_chart(RESULT_DIR, RESULT_DIR)
except Exception as e:
    print(f"Memory chart generation skipped or failed: {e}")

# Step 3: Publish assets (archive only, no commit yet)
now_str = datetime.now().strftime("%Y-%m-%d")
published_assets = None

try:
    published_assets = publish_result_assets(
        repo_root=REPO_ROOT,
        source_dir=BASE_DIR,
        archive_base_dir="benchmark_result",
        remote="origin",
        repo_slug=ASSET_REPO,
        commit_message=ASSET_COMMIT_MESSAGE,
        auto_commit=False,  # Don't commit yet, we'll do it all at once later
    )
    print(
        f"✅ Archived benchmark assets to '{published_assets['archive_dir']}'"
    )
except Exception as e:
    print(f"❌ Benchmark asset archiving failed: {e}")
    sys.exit(1)

# Step 3.5: Append to history database
history_db_path = os.path.join(REPO_ROOT, "benchmark_result", "benchmark_history.db")
try:
    append_benchmark_to_sqlite(
        benchmark_result_dir=os.path.join(REPO_ROOT, "benchmark_result"),
        timestamp_dir=published_assets["archive_dir"],
        db_path=history_db_path
    )
except Exception as e:
    print(f"⚠️ Failed to append to history database: {e}")

# Step 3.6: Generate trend chart from history
trend_chart_path = os.path.join(REPO_ROOT, "benchmark_result", "performance_trend.png")
try:
    result_path = generate_performance_trend_from_sqlite(
        db_path=history_db_path,
        output_path=trend_chart_path
    )
    if result_path:
        print(f"✅ Performance trend chart generated")
except Exception as e:
    print(f"⚠️ Failed to generate trend chart: {e}")

# Step 4: Unified commit and push (archive directory + database + trend chart)
print("\n📝 Performing unified git commit...")
import subprocess

try:
    # Stage all benchmark_result changes
    subprocess.run(
        ["git", "add", "-A", "benchmark_result/"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True
    )
    print("✅ Staged all benchmark_result changes")
    
    # Check if there are staged changes
    check_result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=REPO_ROOT,
        capture_output=True
    )
    
    if check_result.returncode != 0:  # Has changes
        # Commit all changes
        commit_message = f"chore(benchmark): update results, database and trends ({now_str})"
        subprocess.run(
            ["git", "commit", "-m", commit_message],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True
        )
        print(f"✅ Committed all changes with message: '{commit_message}'")
        
        # Set up authentication for push
        github_token = os.getenv("GITHUB_TOKEN")
        original_remote_url = None
        
        if github_token and ASSET_REPO:
            # Save original URL
            try:
                result = subprocess.run(
                    ["git", "remote", "get-url", "origin"],
                    cwd=REPO_ROOT,
                    check=True,
                    capture_output=True,
                    text=True
                )
                original_remote_url = result.stdout.strip()
            except:
                pass
            
            # Set authenticated URL
            auth_url = f"https://{github_token}@github.com/{ASSET_REPO}.git"
            subprocess.run(
                ["git", "remote", "set-url", "origin", auth_url],
                cwd=REPO_ROOT,
                check=True,
                capture_output=True
            )
            print("✅ Authenticated git remote configured")
        
        # Push to remote
        current_branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True
        ).stdout.strip()
        
        subprocess.run(
            ["git", "push", "origin", current_branch],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True
        )
        print(f"✅ Pushed to origin/{current_branch}")
        
        # Restore original remote URL
        if original_remote_url:
            try:
                subprocess.run(
                    ["git", "remote", "set-url", "origin", original_remote_url],
                    cwd=REPO_ROOT,
                    check=True,
                    capture_output=True
                )
            except:
                pass
        
        # Get commit SHA for generating URLs
        commit_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True
        ).stdout.strip()
        print(f"✅ Commit SHA: {commit_sha[:8]}")
        
    else:
        print("⚠️ No changes to commit")
        # Use current commit SHA
        commit_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True
        ).stdout.strip()
        
except subprocess.CalledProcessError as e:
    print(f"❌ Git operation failed: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error during git operations: {e}")
    sys.exit(1)

# Step 5: Generate benchmark comment with correct image URLs
print("\n📝 Generating benchmark comment with image URLs...")

def build_image_url(archive_dir: str, filename: str, commit_sha: str, repo_slug: str) -> str:
    """Build GitHub raw content URL for archived images."""
    return f"https://raw.githubusercontent.com/{repo_slug}/{commit_sha}/benchmark_result/{archive_dir}/{filename}"

# Build image URLs
archive_dir = published_assets["archive_dir"]
read_write_chart_url = build_image_url(archive_dir, "read_write_time_comparison.png", commit_sha, ASSET_REPO)
memory_chart_url = build_image_url(archive_dir, "memory_usage_comparison.png", commit_sha, ASSET_REPO)
trend_chart_url = f"https://raw.githubusercontent.com/{ASSET_REPO}/{commit_sha}/benchmark_result/performance_trend.png"

# Generate markdown comment
with open(COMMENT_MD, "w") as f:
    f.write(f"### 🧪 Benchmark Results ({now_str})\n\n")
    f.write("#### 📊 Performance Overview (TsFile vs Parquet)\n\n")
    f.write(table_header)
    f.write(table_divider)
    f.write("\n".join(table_rows))
    f.write("\n")
    
    f.write("\n#### 📉 Read/Write Time Chart\n\n")
    f.write(f"**read_write_time_comparison.png**\n\n")
    f.write(f"![read_write_time_comparison.png]({read_write_chart_url})\n\n")
    
    f.write("\n#### 📈 Performance Trend\n\n")
    if os.path.exists(trend_chart_path):
        f.write(f"**Historical Performance Trend**\n\n")
        f.write(f"![Performance Trend]({trend_chart_url})\n\n")
    else:
        f.write("*Trend chart will be available after first run*\n\n")
    
    f.write("\n#### 🖼️ Memory Usage Chart\n\n")
    f.write(f"**memory_usage_comparison.png**\n\n")
    f.write(f"![memory_usage_comparison.png]({memory_chart_url})\n\n")

print(f"✅ Benchmark comment written to {COMMENT_MD}")

# Step 6: Upload comment to GitHub
if GITHUB_TOKEN and ISSUE_REPO:
    print("\n📤 Uploading comment to GitHub...")
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    with open(COMMENT_MD, "r") as f:
        comment_body = f.read()
    try:
        response = requests.post(
            f"https://api.github.com/repos/{ISSUE_REPO}/issues/{ISSUE_NUMBER}/comments",
            headers=headers,
            json={"body": comment_body},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        comment = response.json()
        comment_id = comment["id"]
        
        # Update issue body with latest result link
        issue_resp = requests.get(
            f"https://api.github.com/repos/{ISSUE_REPO}/issues/{ISSUE_NUMBER}",
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        issue_resp.raise_for_status()
        original_body = issue_resp.json()["body"]
        link_line = f"[👉 Latest Result](#issuecomment-{comment_id})"
        if "[👉 Latest Result]" in (original_body or ""):
            updated_body = re.sub(r"\[👉 Latest Result\]\(.*?\)", link_line, original_body)
        else:
            updated_body = link_line + "\n\n" + (original_body or "")
        requests.patch(
            f"https://api.github.com/repos/{ISSUE_REPO}/issues/{ISSUE_NUMBER}",
            headers=headers,
            json={"body": updated_body},
            timeout=REQUEST_TIMEOUT,
        ).raise_for_status()
        print("✅ Benchmark comment posted and issue body updated successfully.")
    except Exception as e:
        print(f"⚠️ GitHub upload failed: {e}")
else:
    print("⚠️ GitHub upload skipped (set GITHUB_TOKEN and ISSUE_REPO to enable).")

print("\n✅ Benchmark pipeline completed successfully!")

