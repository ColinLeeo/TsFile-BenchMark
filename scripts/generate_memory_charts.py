import os
import csv
import matplotlib.pyplot as plt
import matplotlib
from pathlib import Path

# Use non-interactive backend for headless environments
matplotlib.use('Agg')

# Configuration
BASE_DIR = "result"
OUTPUT_DIR = "result"
OUTPUT_FILE = "memory_usage_comparison.png"

LANGUAGES = ["java", "python", "cpp"]
LANGUAGE_LABELS = {
    "java": "Java",
    "python": "Python", 
    "cpp": "C++"
}


def read_memory_csv(file_path):
    """Read memory usage CSV and return iteration numbers and memory values."""
    if not os.path.exists(file_path):
        return None, None
    
    iterations = []
    memory_usage = []
    
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Handle different column name formats (iter_num or iter)
            iter_num = row.get('iter_num') or row.get('iter')
            mem_usage = row.get('memory_usage(kb)')
            
            if iter_num and mem_usage:
                iterations.append(int(iter_num))
                memory_usage.append(float(mem_usage) / 1024)  # Convert KB to MB
    
    return iterations, memory_usage


def generate_memory_comparison_chart(base_dir, output_path):
    """Generate a 3-subplot chart comparing TsFile vs Parquet memory usage."""
    
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle('Memory Usage Comparison: TsFile vs Parquet', fontsize=16, fontweight='bold')
    
    for idx, lang in enumerate(LANGUAGES):
        ax = axes[idx]
        
        # Read TsFile data
        tsfile_path = os.path.join(base_dir, f"memory_usage_{lang}.csv")
        tsfile_iter, tsfile_mem = read_memory_csv(tsfile_path)
        
        # Read Parquet data
        parquet_path = os.path.join(base_dir, f"memory_usage_parquet_{lang}.csv")
        parquet_iter, parquet_mem = read_memory_csv(parquet_path)
        
        # Plot data
        if tsfile_iter and tsfile_mem:
            ax.plot(tsfile_iter, tsfile_mem, label='TsFile', 
                   linewidth=2, marker='o', markersize=3, alpha=0.8)
        
        if parquet_iter and parquet_mem:
            ax.plot(parquet_iter, parquet_mem, label='Parquet',
                   linewidth=2, marker='s', markersize=3, alpha=0.8)
        
        # Customize subplot
        ax.set_title(f'{LANGUAGE_LABELS[lang]}', fontsize=14, fontweight='bold')
        ax.set_xlabel('Iteration', fontsize=11)
        ax.set_ylabel('Memory Usage (MB)', fontsize=11)
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    
    # Save figure
    output_file = os.path.join(output_path, OUTPUT_FILE)
    os.makedirs(output_path, exist_ok=True)
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"✅ Memory comparison chart saved to {output_file}")
    
    plt.close()
    return output_file


if __name__ == "__main__":
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    base_dir = os.path.join(repo_root, BASE_DIR)
    output_dir = os.path.join(repo_root, OUTPUT_DIR)
    
    generate_memory_comparison_chart(base_dir, output_dir)
