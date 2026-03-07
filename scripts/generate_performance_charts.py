import os
import matplotlib
import matplotlib.pyplot as plt
import numpy as np


matplotlib.use("Agg")

OUTPUT_FILE = "read_write_time_comparison.png"
LANGUAGE_ORDER = ["Java", "Python", "C++"]


def generate_read_write_time_chart(benchmark_rows, output_dir):
    """
    benchmark_rows item format:
    [format_label, language_label, read_ms, read_speed, write_ms, write_speed, size_mb]
    """
    language_to_values = {
        lang: {
            "TsFile": {"read": 0.0, "write": 0.0},
            "Parquet": {"read": 0.0, "write": 0.0},
        }
        for lang in LANGUAGE_ORDER
    }

    for row in benchmark_rows:
        format_label, language_label = row[0], row[1]
        if language_label not in language_to_values:
            continue
        if format_label not in ("TsFile", "Parquet"):
            continue

        read_ms = float(row[2]) if isinstance(row[2], (int, float)) else 0.0
        write_ms = float(row[4]) if isinstance(row[4], (int, float)) else 0.0
        language_to_values[language_label][format_label]["read"] = round(read_ms / 1000, 2)
        language_to_values[language_label][format_label]["write"] = round(write_ms / 1000, 2)

    x = np.arange(len(LANGUAGE_ORDER))
    width = 0.35

    tsfile_read = [language_to_values[lang]["TsFile"]["read"] for lang in LANGUAGE_ORDER]
    parquet_read = [language_to_values[lang]["Parquet"]["read"] for lang in LANGUAGE_ORDER]
    tsfile_write = [language_to_values[lang]["TsFile"]["write"] for lang in LANGUAGE_ORDER]
    parquet_write = [language_to_values[lang]["Parquet"]["write"] for lang in LANGUAGE_ORDER]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Read/Write Time Comparison: TsFile vs Parquet", fontsize=15, fontweight="bold")

    axes[0].bar(x - width / 2, tsfile_read, width, label="TsFile")
    axes[0].bar(x + width / 2, parquet_read, width, label="Parquet")
    axes[0].set_title("Read Time (s)")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(LANGUAGE_ORDER)
    axes[0].set_ylabel("Time (s)")
    axes[0].grid(True, axis="y", alpha=0.3, linestyle="--")
    axes[0].legend()

    axes[1].bar(x - width / 2, tsfile_write, width, label="TsFile")
    axes[1].bar(x + width / 2, parquet_write, width, label="Parquet")
    axes[1].set_title("Write Time (s)")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(LANGUAGE_ORDER)
    axes[1].set_ylabel("Time (s)")
    axes[1].grid(True, axis="y", alpha=0.3, linestyle="--")
    axes[1].legend()

    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, OUTPUT_FILE)
    plt.savefig(output_file, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Read/write time comparison chart saved to {output_file}")
    return output_file
