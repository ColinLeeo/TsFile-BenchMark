import json
import os
import shutil
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import requests
from git import Repo

from docutils.nodes import row

## 1. Config info
LANGUAGES = ["java", "cpp", "python"]
BASE_DIR = "result"
OUTPUT_DIR = "generated_data"
COMMENTED_MD = os.path.join(OUTPUT_DIR, "benchmark_comment.md")
IMAGE_FILE = os.path.join(OUTPUT_DIR, "memory_usage.png")
BASE_UPLOAD_DIR = "../bench_results"
GITHUB_TOKEN = ""
REPO = "ColinLeeo/tsfile"
ISSUE_NUMBER = "2"
PIC_REPO = "ColinLeeo/TsFile-BenchMark"
ISSUE_NUMBER_PIC = "1"

## 2. Result will be stored at output dir.
os.makedirs(OUTPUT_DIR, exist_ok=True)

## 3. Process data and generate a markdown + png.
benchmark_data = []
for language in LANGUAGES:
    json_path = os.path.join(BASE_DIR, f"results_{language}.json")
    if not os.path.exists(json_path):
        print(f"Couldn't find {json_path}")
        continue
    with open(json_path) as f:
        data = json.load(f)
        benchmark_data.append(
            [language.capitalize(),
             data.get("tsfile_size", ""),
             data.get("prepare_time", ""),
             data.get("write_time", ""),
             data.get("writing_speed", ""),
             data.get("reading_time", ""),
             data.get("reading_speed", "")]
        )


table_header = "| language | tsfile_size | prepare_time | writing_time | writing_speed | reading_time | reading_speed |\n"
table_div    = "|----------|-------------|--------------|--------------|---------------|--------------|---------------|\n"
table_rows = [
    f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} |"
    for row in benchmark_data
]
now_str = datetime.now().strftime("%Y%m%d-%H%M%S")
with open(COMMENTED_MD, "w") as f:
    f.write(f"## Benchmark result of tsfile at {now_str} - commit\n")
    f.write("### write and read result\n")
    f.write(table_header)
    f.write(table_div)
    f.write("\n".join(table_rows))
    f.write("\n\n")

plt.figure(figsize = (10, 6))

for lang in LANGUAGES:
    csv = os.path.join(BASE_DIR, f"memory_usage_{lang}.csv")
    if not os.path.exists(csv):
        print(f"Couldn't find {csv}")
        continue
    df = pd.read_csv(csv)
    plt.plot(df.iloc[:,0], df.iloc[:,1], label=lang.capitalize())

plt.xlabel("iteration")
plt.ylabel("memory_usage(KB)")
plt.title("memory_usage of tsfile when writing data")
plt.legend()
plt.tight_layout()
plt.savefig(IMAGE_FILE)

## commit this results.
repo = Repo("..")
COMMIT_OUTPUT_DIR = os.path.join(repo.working_dir, "bench_results", now_str)
os.makedirs(COMMIT_OUTPUT_DIR, exist_ok=True)
shutil.copy(IMAGE_FILE, COMMIT_OUTPUT_DIR)
shutil.copy(COMMENTED_MD, COMMIT_OUTPUT_DIR)

repo.git.add(COMMIT_OUTPUT_DIR)
repo.index.commit(f"benchmark result {now_str}")
origin = repo.remote(name="origin")
origin.push()


## Update markdown files.
image_url =  f"https://raw.githubusercontent.com/{PIC_REPO}/main/bench_results/{now_str}/memory_usage.png"
with open(COMMENTED_MD, "a") as f:
    f.write("#### memory usage of tsfile when writing data\n")
    f.write(f"![memory usage]({image_url})\n")


headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

with open(COMMENTED_MD, "r") as f:
    comment_body = f.read()

response = requests.post(
    f"https://api.github.com/repos/{REPO}/issues/{ISSUE_NUMBER}/comments",
    headers=headers,
    json={"body": comment_body}
)

response.raise_for_status()

patch_resp = requests.patch(
    f"https://api.github.com/repos/{REPO}/issues/{ISSUE_NUMBER}",
    headers=headers,
    json={"body": comment_body}
)

patch_resp.raise_for_status()
print("✅ Issue body updated with latest timestamp.")
