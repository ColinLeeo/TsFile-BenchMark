import os
import json
import requests
import re
from datetime import datetime
from upload_benchmark_assets import publish_result_assets
from generate_memory_charts import generate_memory_comparison_chart
from generate_performance_charts import generate_read_write_time_chart

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
# Asset repository (for uploading images and benchmark data)
ASSET_REPO = os.getenv("ASSET_REPO", "ColinLeeo/TsFile-BenchMark").strip()
ASSET_PUBLISH_BRANCH = os.getenv("BENCHMARK_ASSET_BRANCH", "").strip()
ASSET_PUBLISH_REMOTE = os.getenv("BENCHMARK_ASSET_REMOTE", "").strip()
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

# Step 3: Prepare markdown comment (text only)
now_str = datetime.now().strftime("%Y-%m-%d")
published_assets = None

if ASSET_PUBLISH_BRANCH:
    try:
        published_assets = publish_result_assets(
            repo_root=REPO_ROOT,
            branch=ASSET_PUBLISH_BRANCH,
            base_dir=BASE_DIR,
            remote=ASSET_PUBLISH_REMOTE or "origin",
            repo_slug=ASSET_REPO,
            commit_message=ASSET_COMMIT_MESSAGE,
        )
        print(
            f"Published benchmark assets to branch '{published_assets['branch']}' "
            f"at commit {published_assets['commit_sha']}."
        )
    except Exception as e:
        print(f"Benchmark asset publish skipped or failed: {e}")

with open(COMMENT_MD, "w") as f:
    f.write(f"### 🧪 Benchmark Results ({now_str})\n\n")
    f.write("#### 📊 Performance Overview (TsFile vs Parquet)\n\n")
    f.write(table_header)
    f.write(table_divider)
    f.write("\n".join(table_rows))
    f.write("\n")

    if published_assets:
        image_urls = published_assets.get("image_urls", {})
        if image_urls:
            f.write("\n#### 📉 Read/Write Time Chart\n\n")
            for relative_path, image_url in image_urls.items():
                if os.path.basename(relative_path) == "read_write_time_comparison.png":
                    image_name = os.path.basename(relative_path)
                    f.write(f"**{image_name}**\n\n![{image_name}]({image_url})\n\n")

            f.write("\n#### 🖼️ Memory Usage Images\n\n")
            for relative_path, image_url in image_urls.items():
                image_name = os.path.basename(relative_path)
                if "memory_usage" in image_name:
                    f.write(f"**{image_name}**\n\n![{image_name}]({image_url})\n\n")

# Step 4: Upload comment to GitHub (optional)
print(f"Report written to {COMMENT_MD}")

if GITHUB_TOKEN and ISSUE_REPO:
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
        print(f"⚠️ GitHub upload skipped or failed: {e}")
else:
    print("⚠️ GitHub upload skipped (set GITHUB_TOKEN and ISSUE_REPO to enable).")
