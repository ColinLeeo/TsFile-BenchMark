import sqlite3
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from pathlib import Path


matplotlib.use("Agg")

OUTPUT_FILE = "performance_trend.png"


def generate_performance_trend_from_sqlite(
    db_path: str = "benchmark_result/benchmark_history.db",
    output_path: str = "result/performance_trend.png"
):
    """
    Generate performance trend chart from SQLite history.
    
    Args:
        db_path: Path to SQLite database
        output_path: Path to save the trend chart
    """
    if not Path(db_path).exists():
        print(f"⚠️ History database not found: {db_path}")
        return None
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Query all historical data, ordered by time
    cursor.execute("""
        SELECT run_datetime, format, language, reading_time, writing_time
        FROM benchmark_history
        ORDER BY run_datetime
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print("⚠️ No data in history database")
        return None
    
    # Group data by format + language
    groups = {}
    for run_dt, fmt, lang, read_time, write_time in rows:
        key = f"{fmt} - {lang}"
        if key not in groups:
            groups[key] = {"dates": [], "read": [], "write": []}
        
        dt = datetime.fromtimestamp(run_dt)
        groups[key]["dates"].append(dt)
        groups[key]["read"].append(read_time or 0)
        groups[key]["write"].append(write_time or 0)
    
    # Create plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle("Performance Trend Over Time", fontsize=16, fontweight="bold")
    
    for key, data in groups.items():
        ax1.plot(data["dates"], data["read"], marker="o", label=key, linewidth=2, markersize=6)
        ax2.plot(data["dates"], data["write"], marker="o", label=key, linewidth=2, markersize=6)
    
    ax1.set_xlabel("Date", fontsize=11)
    ax1.set_ylabel("Read Time (s)", fontsize=11)
    ax1.set_title("Read Time Trend", fontsize=13, fontweight="bold")
    ax1.legend(loc="best")
    ax1.grid(True, alpha=0.3, linestyle="--")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha="right")
    
    ax2.set_xlabel("Date", fontsize=11)
    ax2.set_ylabel("Write Time (s)", fontsize=11)
    ax2.set_title("Write Time Trend", fontsize=13, fontweight="bold")
    ax2.legend(loc="best")
    ax2.grid(True, alpha=0.3, linestyle="--")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha="right")
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Performance trend chart saved to {output_path}")
    return output_path
