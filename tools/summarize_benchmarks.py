import argparse
import json
import sys
import os

def print_summary(results1, results2, engine_name):
    """Prints easy-to-read summaries comparing a single engine's results between two files."""

    # Extract benchmark data for the specified engine from both files
    engine_benchmarks1 = [r for r in results1.get("engine_benchmarks", []) if r.get("engine") == engine_name]
    flask_app_benchmarks1 = [r for r in results1.get("flask_app_benchmarks", []) if r.get("engine") == f"Flask_App_with_{engine_name}"]
    engine_benchmarks2 = [r for r in results2.get("engine_benchmarks", []) if r.get("engine") == engine_name]
    flask_app_benchmarks2 = [r for r in results2.get("flask_app_benchmarks", []) if r.get("engine") == f"Flask_App_with_{engine_name}"]

    # Combine and group results by dataset size and operation for easy lookup
    results1_lookup = {}
    for r in engine_benchmarks1 + flask_app_benchmarks1:
        key = (r.get("dataset_size"), r.get("operation"))
        if key[0] is not None and key[1] is not None:
            results1_lookup[key] = r

    results2_lookup = {}
    for r in engine_benchmarks2 + flask_app_benchmarks2:
        key = (r.get("dataset_size"), r.get("operation"))
        if key[0] is not None and key[1] is not None:
            results2_lookup[key] = r

    # Get all unique dataset sizes and operations present for this engine in either file
    all_sizes = sorted(list(set([r.get("dataset_size") for r in engine_benchmarks1 + flask_app_benchmarks1 + engine_benchmarks2 + flask_app_benchmarks2 if r.get("dataset_size") is not None])))
    all_operations = sorted(list(set([r.get("operation") for r in engine_benchmarks1 + flask_app_benchmarks1 + engine_benchmarks2 + flask_app_benchmarks2 if r.get("operation") is not None])))

    # Print Summary
    print(f"\n--- Benchmark Summary for Engine: {engine_name} (Comparison: New vs Base) ---")

    for size in all_sizes:
        print(f"\nDataset Size: {size} entries")
        print(f"{'Operation':<30} {'Base Time (s)':<20} {'New Time (s)':<20} {'Time Delta (%)':<18} {'Base Mem (MB)':<20} {'New Mem (MB)':<20} {'Mem Delta (%)':<18}")
        print(f"{'-'*30:<30} {'-'*20:<20} {'-'*20:<20} {'-'*18:<18} {'-'*20:<20} {'-'*20:<20} {'-'*18:<18}")

        for operation in all_operations:
            res1 = results1_lookup.get((size, operation))
            res2 = results2_lookup.get((size, operation))

            time1 = res1["avg_time_seconds"] if res1 else 0
            mem1 = (res1["avg_peak_memory_bytes"] / (1024 * 1024)) if res1 else 0
            time2 = res2["avg_time_seconds"] if res2 else 0
            mem2 = (res2["avg_peak_memory_bytes"] / (1024 * 1024)) if res2 else 0

            time_delta_percent = ((time2 - time1) / time1 * 100) if time1 != 0 else float('inf')
            mem_delta_percent = ((mem2 - mem1) / mem1 * 100) if mem1 != 0 else float('inf')

            time_delta_percent_str = f"{time_delta_percent:.2f}" if time_delta_percent != float('inf') else "INF"
            mem_delta_percent_str = f"{mem_delta_percent:.2f}" if mem_delta_percent != float('inf') else "INF"

            print(f"{operation:<30} {time1:<20.4f} {time2:<20.4f} {time_delta_percent_str:<18} {mem1:<20.2f} {mem2:<20.2f} {mem_delta_percent_str:<18}")

    print("\n-------------------------")


def main():
    parser = argparse.ArgumentParser(description="Summarize and compare benchmark results for a single engine between two JSON files.")
    parser.add_argument("file1", type=str,
                        help="Path to the first benchmark results JSON file (baseline).")
    parser.add_argument("file2", type=str,
                        help="Path to the second benchmark results JSON file (to compare).")
    parser.add_argument("engine_name", type=str,
                        help="Name of the engine to compare (e.g., 'Sqlite-File').")

    args = parser.parse_args()

    try:
        with open(args.file1, 'r') as f:
            results1 = json.load(f)
        with open(args.file2, 'r') as f:
            results2 = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading benchmark results file: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from benchmark results file: {e}", file=sys.stderr)
        sys.exit(1)

    # print_summary now handles the comparison logic directly from results1 and results2
    print_summary(results1, results2, args.engine_name)


if __name__ == "__main__":
    main()