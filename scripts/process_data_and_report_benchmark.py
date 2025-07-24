import json
import os
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd

from docutils.nodes import row

LANGUAGES = ["java", "cpp", "python"]
BASE_DIR = "result"
OUTPUT_DIR = "generated_data"
COMMENTED_MD = os.path.join(OUTPUT_DIR, "benchmark_comment.md")
IMAGE_FILE = os.path.join(OUTPUT_DIR, "memory_usage.png")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = "ColinLeeo/tsfile"
ISSUE_NUMBER = "460"

os.makedirs(OUTPUT_DIR, exist_ok=True)

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
             data.get("writing_time", ""),
             data.get("writing_speed", ""),
             data.get("reading_time", ""),
             data.get("reading_speed", "")]
        )



table_header = "| language | tsfile_size | prepare_time | writing_time | writing_speed | reading_time | reading_speed |"
table_div    = "|----------|-------------|--------------|--------------|---------------|--------------|---------------|"
table_rows = [
    f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |"
    for row in benchmark_data
]

plt.figure(figsize = (10, 6))

for lang in LANGUAGES:
    csv = os.path.join(OUTPUT_DIR, "memory_usage_{lang}.csv")
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

now_str = datetime.now().strftime("%Y%m%d-%H%M%S")
with open(COMMENTED_MD, "w") as f:
    f.write(f"## Benchmark result of tsfile at {now_str} - commit\n")
    f.write("write and read result\n")
    f.write(table_header)
    f.write(table_div)
    f.write("\n".join(table_rows))
    f.write("\n")
    f.write("memory usage of tsfile when writing data\n")

