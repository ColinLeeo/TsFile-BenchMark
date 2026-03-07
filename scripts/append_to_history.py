import sqlite3
import json
from pathlib import Path
from datetime import datetime


def init_history_db(db_path: str):
    """Initialize SQLite database with schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            run_datetime INTEGER NOT NULL,
            format TEXT NOT NULL,
            language TEXT NOT NULL,
            reading_time REAL,
            writing_time REAL,
            reading_speed REAL,
            writing_speed REAL,
            file_size_mb REAL,
            UNIQUE(timestamp, format, language)
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_run_datetime 
        ON benchmark_history(run_datetime)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_format_lang 
        ON benchmark_history(format, language)
    """)
    
    conn.commit()
    conn.close()


def append_benchmark_to_sqlite(
    benchmark_result_dir: str,
    timestamp_dir: str,
    db_path: str = "benchmark_result/benchmark_history.db"
):
    """
    Append current benchmark results to SQLite database.
    
    Args:
        benchmark_result_dir: benchmark_result/ directory path
        timestamp_dir: Current run timestamp directory name, e.g., "2024-03-07_14-30-25"
        db_path: SQLite database path
    """
    result_dir = Path(benchmark_result_dir) / timestamp_dir
    if not result_dir.exists():
        print(f"⚠️ Result directory not found: {result_dir}")
        return
    
    # Initialize database if not exists
    init_history_db(db_path)
    
    # Parse timestamp
    dt = datetime.strptime(timestamp_dir, "%Y-%m-%d_%H-%M-%S")
    run_datetime = int(dt.timestamp())
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    records_added = 0
    
    # Process each result JSON
    for json_file in result_dir.glob("results_*.json"):
        with open(json_file) as f:
            data = json.load(f)
        
        # Parse format and language
        name = json_file.stem
        if "parquet" in name:
            fmt = "Parquet"
            lang = name.split("_")[-1].capitalize()
        else:
            fmt = "TsFile"
            lang = name.split("_")[-1].capitalize()
        
        # Insert or ignore if duplicate
        try:
            cursor.execute("""
                INSERT INTO benchmark_history 
                (timestamp, run_datetime, format, language, 
                 reading_time, writing_time, reading_speed, writing_speed, file_size_mb)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp_dir,
                run_datetime,
                fmt,
                lang,
                data.get("reading_time"),
                data.get("writing_time"),
                data.get("reading_speed"),
                data.get("writing_speed"),
                data.get("file_size_mb"),
            ))
            records_added += 1
        except sqlite3.IntegrityError:
            print(f"⚠️ Duplicate entry: {fmt} - {lang} at {timestamp_dir}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Added {records_added} records to {db_path}")
