import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import requests
import re
from datetime import datetime

# ------------------- Configuration -------------------
BASE_DIR = "result"
OUTPUT_DIR = "generated"
COMMENT_MD = os.path.join(OUTPUT_DIR, "benchmark_comment.md")
IMAGE_FILE = os.path.join(OUTPUT_DIR, "memory_usage.png")

# Flat layout (Docker output): result/results_java.json, result/results_parquet_java.json, result/memory_usage_*.csv
TSFILE_JSON = ["results_java.json", "results_python.json"]
PARQUET_JSON = ["results_parquet_java.json", "results_parquet_python.json", "results_parquet_cpp.json"]

# GitHub settings (fill in before use)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
try:
    if not GITHUB_TOKEN:
        GITHUB_TOKEN = open(os.path.expanduser("~/.github_token")).read().strip()
except FileNotFoundError:
    GITHUB_TOKEN = ""
REPO = "your_user/your_repo"  # Replace with your repo, e.g. "apache/tsfile"
ISSUE_NUMBER = 1              # Replace with your issue number (integer)
# -----------------------------------------------------

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_flat_result(json_filename, format_label, lang_label):
    """Load one benchmark JSON from BASE_DIR (flat). Returns row or None if missing."""
    path = os.path.join(BASE_DIR, json_filename)
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        data = json.load(f)
    # Our keys: reading_time (s), reading_speed, write_time (s), writing_speed, tsfile_size (KB)
    read_ms = round(data.get("reading_time", 0) * 1000, 2) if data.get("reading_time") is not None else ""
    read_speed = data.get("reading_speed", "")
    write_ms = round(data.get("write_time", 0) * 1000, 2) if data.get("write_time") is not None else ""
    write_speed = data.get("writing_speed", "")
    size_mb = round(data.get("tsfile_size", 0) / 1024, 2) if data.get("tsfile_size") is not None else ""
    return [format_label, lang_label, read_ms, read_speed, write_ms, write_speed, size_mb]


# Step 1: Parse benchmark data (TsFile + Parquet, flat layout)
benchmark_data = []
for fname in TSFILE_JSON:
    lang = "java" if "java" in fname else "python"
    row = load_flat_result(fname, "TsFile", lang.capitalize())
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
        f"No benchmark JSON found under {BASE_DIR}. "
        f"Expected e.g. {TSFILE_JSON}, {PARQUET_JSON}"
    )

# Step 2: Generate markdown table
table_header = "| 格式   | 语言    | 读取时间(ms) | 读取速度(points/s) | 写入时间(ms) | 写入速度(points/s) | 文件大小(MB) |\n"
table_divider = "|--------|---------|--------------|---------------------|---------------|---------------------|---------------|\n"
table_rows = [
    f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} |"
    for row in benchmark_data
]

# Step 3: Plot memory usage — one subplot per language, TsFile vs Parquet in same chart
fig, axes = plt.subplots(1, 3, figsize=(14, 5))
per_lang = [
    ("Java", "memory_usage_java.csv", "memory_usage_parquet_java.csv"),
    ("Python", "memory_usage_python.csv", "memory_usage_parquet_python.csv"),
    ("C++", "memory_usage_cpp.csv", "memory_usage_parquet_cpp.csv"),
]
for ax, (lang, tsfile_csv, parquet_csv) in zip(axes, per_lang):
    for csv_name, label in [(tsfile_csv, "TsFile"), (parquet_csv, "Parquet")]:
        if csv_name is None:
            continue
        csv_path = os.path.join(BASE_DIR, csv_name)
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            xcol, ycol = df.columns[0], df.columns[1]
            ax.plot(df[xcol], df[ycol], label=label)
    ax.set_xlabel("迭代")
    ax.set_ylabel("内存占用 (KB)")
    ax.set_title(f"{lang} — TsFile vs Parquet" if parquet_csv else f"{lang} (TsFile)")
    ax.legend()
plt.suptitle("内存占用曲线（按语言）", y=1.02)
plt.tight_layout()
plt.savefig(IMAGE_FILE)

# Step 4: Prepare markdown comment
now_str = datetime.now().strftime("%Y-%m-%d")
with open(COMMENT_MD, "w") as f:
    f.write(f"### 🧪 Benchmark 结果（{now_str}）\n\n")
    f.write("#### 📊 性能概览（TsFile vs Parquet）\n\n")
    f.write(table_header)
    f.write(table_divider)
    f.write("\n".join(table_rows))
    f.write("\n\n")
    f.write("#### 🧠 内存占用曲线\n\n")
    f.write(f"![memory usage]({os.path.basename(IMAGE_FILE)})\n")

# Step 5: Upload comment to GitHub (optional)
print(f"✅ Report written to {COMMENT_MD} and {IMAGE_FILE}")

if GITHUB_TOKEN and REPO and REPO != "your_user/your_repo":
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    with open(COMMENT_MD, "r") as f:
        comment_body = f.read()
    try:
        response = requests.post(
            f"https://api.github.com/repos/{REPO}/issues/{ISSUE_NUMBER}/comments",
            headers=headers,
            json={"body": comment_body}
        )
        response.raise_for_status()
        comment = response.json()
        comment_id = comment["id"]
        issue_resp = requests.get(
            f"https://api.github.com/repos/{REPO}/issues/{ISSUE_NUMBER}",
            headers=headers
        )
        issue_resp.raise_for_status()
        original_body = issue_resp.json()["body"]
        link_line = f"[👉 最新结果](#issuecomment-{comment_id})"
        if "[👉 最新结果]" in (original_body or ""):
            updated_body = re.sub(r"\[👉 最新结果\]\(.*?\)", link_line, original_body)
        else:
            updated_body = link_line + "\n\n" + (original_body or "")
        requests.patch(
            f"https://api.github.com/repos/{REPO}/issues/{ISSUE_NUMBER}",
            headers=headers,
            json={"body": updated_body}
        ).raise_for_status()
        print("✅ Benchmark comment posted and issue body updated successfully.")
    except Exception as e:
        print(f"⚠️ GitHub upload skipped or failed: {e}")
else:
    print("⚠️ GitHub upload skipped (set GITHUB_TOKEN and REPO to enable).")
