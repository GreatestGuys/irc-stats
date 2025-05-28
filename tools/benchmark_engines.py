import argparse
import datetime
import json
import os
import random
import subprocess
import time
import tracemalloc
import gc
import sys

# Ensure the web directory is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from web.logs import InMemoryLogQueryEngine, SQLiteLogQueryEngine
from web import app, APP_STATIC # APP_STATIC might not be directly used but good to have app context
import web.logs # Import the web.logs module to access its global log_engine instance

# Fixed parameters for data generation
DATA_GEN_START_DATE = "2013-01-01"
DATA_GEN_END_DATE = "2025-03-31"
SQLITE_BATCH_SIZE = 1000 # For SQLiteLogQueryEngine instantiation

REPRESENTATIVE_QUERIES = [
    {"name": "startup", "method_to_call": None}, # Special case for startup
    {
        "name": "query_simple_keyword",
        "method_to_call": "query_logs",
        "query_args": {"s": "request"} # "request" is a common word in VOCABULARY
    },
    {
        "name": "query_regex_keyword",
        "method_to_call": "query_logs",
        "query_args": {"s": "err[oa]r\\w+", "ignore_case": True} # error/errors
    },
    {
        "name": "query_nick_filter",
        "method_to_call": "query_logs",
        "query_args": {"s": "user", "nick": "Cosmo"} # "user" is common, User1 will exist
    },
    {
        "name": "query_cumulative",
        "method_to_call": "query_logs",
        "query_args": {"s": "response", "cumulative": True} # "response" is common
    },
    {
        "name": "query_normalize",
        "method_to_call": "query_logs",
        "query_args": {"s": "login", "normalize": True, "normalize_type": "trailing_avg_1"} # "login" is in VOCABULARY
    },
    {
        "name": "count_simple_keyword",
        "method_to_call": "count_occurrences",
        "query_args": {"s": "file"} # "file" is in VOCABULARY
    },
    {
        "name": "count_regex_keyword_ignore_case",
        "method_to_call": "count_occurrences",
        "query_args": {"s": "Serv[ie]ce\\d+", "ignore_case": True} # e.g. Service123, Servoce456
    },
    {
        "name": "get_valid_days",
        "method_to_call": "get_valid_days",
        "query_args": {}
    },
    {
        "name": "get_logs_by_day_specific",
        "method_to_call": "get_logs_by_day",
        "query_args": {"year": 2019, "month": 6, "day": 15}
    },
    {
        "name": "search_day_logs_simple",
        "method_to_call": "search_day_logs",
        "query_args": {"s": "database"} # "database" is in VOCABULARY
    },
    {
        "name": "search_results_to_chart_simple",
        "method_to_call": "search_results_to_chart",
        "query_args": {"s": "database", "ignore_case": True} # "database" is in VOCABULARY
    },
    {
        "name": "get_trending",
        "method_to_call": "get_trending",
        "query_args": {}
    },
]

def generate_dataset(size, data_gen_script_path, output_file_path, seed):
    """Generates a dataset using generate_log_data.py."""
    print(f"Generating dataset of size {size} at {output_file_path} with seed {seed}...")
    args = [
        sys.executable, # Use the current Python interpreter
        data_gen_script_path,
        "--num-entries", str(size),
        "--start-date", DATA_GEN_START_DATE,
        "--end-date", DATA_GEN_END_DATE,
        "--output-file", output_file_path,
        "--seed", str(seed)
    ]
    try:
        process = subprocess.run(args, capture_output=True, text=True, check=True)
        print(f"Dataset generation successful for size {size}.")
        # print(process.stdout) # Optional: print stdout from script
        # if process.stderr:
        # print(f"Stderr from data gen (size {size}):\n{process.stderr}", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error generating dataset of size {size}: {e}", file=sys.stderr)
        print(f"Stdout from data gen (size {size}):\n{e.stdout}", file=sys.stderr)
        print(f"Stderr from data gen (size {size}):\n{e.stderr}", file=sys.stderr)
        return False
    return True

def run_single_test(engine_instance, method_name, query_args_dict):
    """Runs a single query test and measures time and memory."""
    if not engine_instance and method_name is not None : # Should not happen if called correctly
        raise ValueError("Engine instance is None for a query method call.")

    tracemalloc.start()
    t_start = time.perf_counter()

    result_data = None # To store actual query result if needed for validation (not used now)
    if method_name: # For query methods
        method = getattr(engine_instance, method_name)
        result_data = method(**query_args_dict)
    # If method_name is None, it's handled by the caller (startup time)

    t_end = time.perf_counter()
    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # Optionally, do something with result_data to ensure it's computed (e.g. len(result_data))
    # This is important if engine methods are lazy. Assume they are not for now.
    if result_data is not None and hasattr(result_data, '__len__'):
        pass # e.g. print(f"Query {method_name} returned {len(result_data)} items.")

    return t_end - t_start, peak_mem


def main():
    parser = argparse.ArgumentParser(description="Benchmark Log Query Engines.")
    parser.add_argument("--dataset-sizes", type=str, default="10000",
                        help="Comma-separated string of log entry counts (e.g., '10000,100000').")
    parser.add_argument("--output-results-file", type=str, default="benchmark_results.json",
                        help="File to save benchmark results.")
    parser.add_argument("--data-gen-script", type=str, default="tools/generate_log_data.py",
                        help="Path to the data generation script.")
    parser.add_argument("--temp-data-file-prefix", type=str, default="temp_benchmark_data",
                        help="Prefix for temporary log data files.")
    parser.add_argument("--runs-per-test", type=int, default=3,
                        help="Number of times to run each specific benchmark test for averaging.")
    parser.add_argument("--log-file", type=str,
                        help="Optional: Path to a specific log file to use instead of generating one.")

    args = parser.parse_args()

    app.testing = True # Set Flask app to testing mode

    dataset_sizes = [int(s.strip()) for s in args.dataset_sizes.split(',')]
    engine_classes = {
        "InMemory": InMemoryLogQueryEngine,
        "SQLite": SQLiteLogQueryEngine
    }

    engine_run_results = [] # Store results from every single run for engines
    flask_app_run_results = [] # Store results from every single run for Flask app

    base_seed = random.randint(1, 10000) # Use a base seed to make multiple script executions somewhat varied but internally consistent

    for size_idx, size in enumerate(dataset_sizes):
        # Determine the log file to use
        if args.log_file:
            temp_data_file = args.log_file
            print(f"Using specified log file: {temp_data_file} for dataset size {size}.")
            # For specified log file, we assume its size matches the first dataset_size if multiple are given
            # Or, if only one dataset_size is given, it's for that.
            # If multiple sizes are given with a single log file, it will use the same file for all.
            # This might not be ideal for "size" interpretation but is consistent with the request.
            if size_idx > 0:
                print(f"WARNING: Multiple dataset sizes specified with a single --log-file. Using {temp_data_file} for all sizes.", file=sys.stderr)
        else:
            dataset_seed = base_seed + size_idx # Unique seed for each dataset size
            temp_data_file = f"{args.temp_data_file_prefix}_{size}.json"
            if not generate_dataset(size, args.data_gen_script, temp_data_file, dataset_seed):
                print(f"Skipping benchmark for dataset size {size} due to generation error.", file=sys.stderr)
                continue

        try:
            for engine_name, EngineClass in engine_classes.items():
                print(f"\nBenchmarking {engine_name} with dataset size {size} (File: {temp_data_file})")

                for run_num in range(1, args.runs_per_test + 1):
                    print(f"  Run {run_num}/{args.runs_per_test} for {engine_name} on {size} entries...")

                    # 1. Benchmark Startup Time & Memory
                    print(f"    Benchmarking startup...")
                    tracemalloc.start() # Start tracing for instantiation
                    startup_t_start = time.perf_counter()

                    current_engine = None # Define before try block
                    try:
                        if engine_name == "SQLite":
                            current_engine = EngineClass(log_file_path=temp_data_file, batch_size=SQLITE_BATCH_SIZE)
                        else:
                            current_engine = EngineClass(log_file_path=temp_data_file)
                    except Exception as e:
                        print(f"      ERROR instantiating {engine_name}: {e}", file=sys.stderr)
                        tracemalloc.stop()
                        continue # Skip to next run or engine

                    startup_t_end = time.perf_counter()
                    startup_current_mem, startup_peak_mem = tracemalloc.get_traced_memory()
                    tracemalloc.stop()

                    engine_run_results.append({
                        "engine": engine_name,
                        "dataset_size": size,
                        "run": run_num,
                        "operation": "startup",
                        "time_seconds": startup_t_end - startup_t_start,
                        "peak_memory_bytes": startup_peak_mem
                    })
                    print(f"      Startup: time={startup_t_end - startup_t_start:.4f}s, peak_mem={startup_peak_mem / (1024*1024):.2f}MB")

                    # 2. Benchmark Queries
                    if current_engine: # If instantiation was successful
                        for query_def in REPRESENTATIVE_QUERIES:
                            if query_def["name"] == "startup": continue # Already handled

                            print(f"    Benchmarking query: {query_def['name']}...")

                            current_engine.clear_all_caches()

                            try:
                                q_time, q_peak_mem = run_single_test(current_engine, query_def["method_to_call"], query_def["query_args"])
                                engine_run_results.append({
                                    "engine": engine_name,
                                    "dataset_size": size,
                                    "run": run_num,
                                    "operation": query_def["name"],
                                    "time_seconds": q_time,
                                    "peak_memory_bytes": q_peak_mem
                                })
                                print(f"      Query {query_def['name']}: time={q_time:.4f}s, peak_mem={q_peak_mem / (1024*1024):.2f}MB")
                            except Exception as e:
                                print(f"      ERROR running query {query_def['name']} for {engine_name}: {e}", file=sys.stderr)

                        # Cleanup engine instance after all queries for this run
                        del current_engine
                        current_engine = None
                        gc.collect()

                # --- Full Flask App Benchmarking for current engine_name ---
                print(f"\n--- Benchmarking Full Flask Application with {engine_name} engine (Dataset Size: {size}) ---")
                # app is already imported globally from web import app, APP_STATIC

                # Define representative HTTP routes/endpoints to test
                # Mimic tests in tests/test_integration.py
                REPRESENTATIVE_ROUTES = [
                    {"name": "home_route", "path": "/"},
                    {"name": "query_route_no_params", "path": "/query"},
                    {"name": "query_route_with_params", "path": "/query?label=test&regexp=hello"},
                    {"name": "browse_route", "path": "/browse"},
                    {"name": "browse_day_route_specific_date", "path": "/browse/2023/03/15"},
                    {"name": "browse_day_route_invalid_date", "path": "/browse/2000/01/01"},
                    {"name": "search_route_no_params", "path": "/search"},
                    {"name": "search_route_with_params", "path": "/search?q=test&ignore_case=true"},
                ]

                # Temporarily replace the app's log_engine instance
                original_app_log_engine_internal = web.logs.log_engine
                try:
                    if engine_name == "SQLite":
                        web.logs.log_engine = SQLiteLogQueryEngine(log_file_path=temp_data_file, batch_size=SQLITE_BATCH_SIZE)
                    else: # InMemory
                        web.logs.log_engine = InMemoryLogQueryEngine(log_file_path=temp_data_file)

                    with app.test_client() as client:
                        for run_num_flask in range(1, args.runs_per_test + 1):
                            print(f"  Run {run_num_flask}/{args.runs_per_test} for Flask App with {engine_name}...")
                            for route_def in REPRESENTATIVE_ROUTES:

                                web.logs.log_engine.clear_all_caches()

                                print(f"    Benchmarking route: {route_def['name']} ({route_def['path']})...")
                                tracemalloc.start()
                                t_start = time.perf_counter()

                                try:
                                    response = client.get(route_def["path"])
                                    # Ensure response content is consumed to get full memory usage
                                    _ = response.data
                                    status_code = response.status_code
                                except Exception as e:
                                    print(f"      ERROR accessing route {route_def['name']}: {e}", file=sys.stderr)
                                    status_code = -1
                                    response = None

                                t_end = time.perf_counter()
                                current_mem, peak_mem = tracemalloc.get_traced_memory()
                                tracemalloc.stop()

                                flask_app_run_results.append({
                                    "engine": f"Flask_App_with_{engine_name}", # Differentiate Flask app runs by underlying engine
                                    "dataset_size": size, # Associate with the current dataset size
                                    "run": run_num_flask,
                                    "operation": route_def["name"],
                                    "time_seconds": t_end - t_start,
                                    "peak_memory_bytes": peak_mem,
                                    "status_code": status_code
                                })
                                print(f"      Route {route_def['name']}: time={t_end - t_start:.4f}s, peak_mem={peak_mem / (1024*1024):.2f}MB, status={status_code}")
                            gc.collect() # Clean up after each run
                finally:
                    # Restore the original web.logs.log_engine instance
                    web.logs.log_engine = original_app_log_engine_internal
                    gc.collect() # Ensure cleanup after restoring

        finally:
            # Only remove if the file was generated, not if it was provided by the user
            if not args.log_file and os.path.exists(temp_data_file):
                print(f"Cleaning up temporary data file: {temp_data_file}")
                try:
                    os.remove(temp_data_file)
                except OSError as e:
                    print(f"Error removing temporary file {temp_data_file}: {e}", file=sys.stderr)

    # Aggregate results separately
    print("\nAggregating Engine results...")
    aggregated_engine_results = {}
    for record in engine_run_results:
        key = (record["engine"], record["dataset_size"], record["operation"])
        if key not in aggregated_engine_results:
            aggregated_engine_results[key] = {"times": [], "mems": []}
        aggregated_engine_results[key]["times"].append(record["time_seconds"])
        aggregated_engine_results[key]["mems"].append(record["peak_memory_bytes"])

    final_engine_results_summary = []
    for key, data in aggregated_engine_results.items():
        avg_time = sum(data["times"]) / len(data["times"])
        avg_mem = sum(data["mems"]) / len(data["mems"])
        final_engine_results_summary.append({
            "engine": key[0],
            "dataset_size": key[1],
            "operation": key[2],
            "avg_time_seconds": avg_time,
            "avg_peak_memory_bytes": avg_mem,
            "runs": len(data["times"])
        })
    final_engine_results_summary.sort(key=lambda x: (x["dataset_size"], x["engine"], x["operation"]))


    print("\nAggregating Flask App results...")
    aggregated_flask_app_results = {}
    for record in flask_app_run_results:
        key = (record["engine"], record["dataset_size"], record["operation"])
        if key not in aggregated_flask_app_results:
            aggregated_flask_app_results[key] = {"times": [], "mems": []}
        aggregated_flask_app_results[key]["times"].append(record["time_seconds"])
        aggregated_flask_app_results[key]["mems"].append(record["peak_memory_bytes"])

    final_flask_app_results_summary = []
    for key, data in aggregated_flask_app_results.items():
        avg_time = sum(data["times"]) / len(data["times"])
        avg_mem = sum(data["mems"]) / len(data["mems"])
        final_flask_app_results_summary.append({
            "engine": key[0],
            "dataset_size": key[1],
            "operation": key[2],
            "avg_time_seconds": avg_time,
            "avg_peak_memory_bytes": avg_mem,
            "runs": len(data["times"])
        })
    final_flask_app_results_summary.sort(key=lambda x: (x["dataset_size"], x["engine"], x["operation"]))


    # Write aggregated results to file
    try:
        with open(args.output_results_file, 'w') as f:
            json.dump({
                "engine_benchmarks": final_engine_results_summary,
                "flask_app_benchmarks": final_flask_app_results_summary
            }, f, indent=2)
        print(f"Benchmark results saved to {args.output_results_file}")
    except IOError as e:
        print(f"Error writing results to file: {e}", file=sys.stderr)

    # Print the summaries
    print_summary(final_engine_results_summary, final_flask_app_results_summary)

def print_summary(engine_results, flask_app_results):
    """Prints easy-to-read summaries of the benchmark results."""

    # Print Engine Summary
    print("\n--- Log Engine Benchmark Summary ---")

    # Group results by dataset size
    engine_results_by_size = {}
    for r in engine_results:
        size = r["dataset_size"]
        if size not in engine_results_by_size:
            engine_results_by_size[size] = []
        engine_results_by_size[size].append(r)

    for size in sorted(engine_results_by_size.keys()):
        print(f"\nDataset Size: {size} entries")
        print(f"{'Operation':<30} {'InMemory Time (s)':<20} {'SQLite Time (s)':<20} {'Time Delta (%)':<18} {'InMemory Mem (MB)':<20} {'SQLite Mem (MB)':<20} {'Mem Delta (%)':<18}")
        print(f"{'-'*30:<30} {'-'*20:<20} {'-'*20:<20} {'-'*18:<18} {'-'*20:<20} {'-'*20:<20} {'-'*18:<18}")

        operations_data = {}
        for r in engine_results_by_size[size]:
            operation = r["operation"]
            if operation not in operations_data:
                operations_data[operation] = {}
            operations_data[operation][r["engine"]] = r

        for operation in sorted(operations_data.keys()):
            inmemory_res = operations_data[operation].get("InMemory")
            sqlite_res = operations_data[operation].get("SQLite")

            inmemory_time = inmemory_res["avg_time_seconds"] if inmemory_res else 0
            inmemory_mem = (inmemory_res["avg_peak_memory_bytes"] / (1024 * 1024)) if inmemory_res else 0
            sqlite_time = sqlite_res["avg_time_seconds"] if sqlite_res else 0
            sqlite_mem = (sqlite_res["avg_peak_memory_bytes"] / (1024 * 1024)) if sqlite_res else 0

            time_delta_percent = ((sqlite_time - inmemory_time) / inmemory_time * 100) if inmemory_time != 0 else float('inf')
            mem_delta_percent = ((sqlite_mem - inmemory_mem) / inmemory_mem * 100) if inmemory_mem != 0 else float('inf')

            time_delta_percent_str = f"{time_delta_percent:.2f}" if time_delta_percent != float('inf') else "INF"
            mem_delta_percent_str = f"{mem_delta_percent:.2f}" if mem_delta_percent != float('inf') else "INF"

            print(f"{operation:<30} {inmemory_time:<20.4f} {sqlite_time:<20.4f} {time_delta_percent_str:<18} {inmemory_mem:<20.2f} {sqlite_mem:<20.2f} {mem_delta_percent_str:<18}")
    print("\n-------------------------")


    # Print Flask App Summary
    print("\n--- Flask App Benchmark Summary ---")
    flask_app_results_by_size = {}
    for r in flask_app_results:
        size = r["dataset_size"]
        if size not in flask_app_results_by_size:
            flask_app_results_by_size[size] = []
        flask_app_results_by_size[size].append(r)

    for size in sorted(flask_app_results_by_size.keys()):
        print(f"\nDataset Size: {size} entries (Flask App)")
        # Header for Flask App benchmarks, comparing InMemory vs SQLite backend
        print(f"{'Operation':<30} {'Flask App (InMemory) Time (s)':<30} {'Flask App (SQLite) Time (s)':<30} {'Time Delta (%)':<18} {'Flask App (InMemory) Mem (MB)':<30} {'Flask App (SQLite) Mem (MB)':<30} {'Mem Delta (%)':<18}")
        print(f"{'-'*30:<30} {'-'*30:<30} {'-'*30:<30} {'-'*18:<18} {'-'*30:<30} {'-'*30:<30} {'-'*18:<18}")

        operations_data = {}
        for r in flask_app_results_by_size[size]:
            operation = r["operation"]
            if operation not in operations_data:
                operations_data[operation] = {}
            operations_data[operation][r["engine"]] = r # Key will be "Flask_App_with_InMemory" or "Flask_App_with_SQLite"

        for operation in sorted(operations_data.keys()):
            flask_inmemory_res = operations_data[operation].get("Flask_App_with_InMemory")
            flask_sqlite_res = operations_data[operation].get("Flask_App_with_SQLite")

            flask_inmemory_time = flask_inmemory_res["avg_time_seconds"] if flask_inmemory_res else 0
            flask_inmemory_mem = (flask_inmemory_res["avg_peak_memory_bytes"] / (1024 * 1024)) if flask_inmemory_res else 0
            flask_sqlite_time = flask_sqlite_res["avg_time_seconds"] if flask_sqlite_res else 0
            flask_sqlite_mem = (flask_sqlite_res["avg_peak_memory_bytes"] / (1024 * 1024)) if flask_sqlite_res else 0

            time_delta_percent = ((flask_sqlite_time - flask_inmemory_time) / flask_inmemory_time * 100) if flask_inmemory_time != 0 else float('inf')
            mem_delta_percent = ((flask_sqlite_mem - flask_inmemory_mem) / flask_inmemory_mem * 100) if flask_inmemory_mem != 0 else float('inf')

            time_delta_percent_str = f"{time_delta_percent:.2f}" if time_delta_percent != float('inf') else "INF"
            mem_delta_percent_str = f"{mem_delta_percent:.2f}" if mem_delta_percent != float('inf') else "INF"

            print(f"{operation:<30} {flask_inmemory_time:<30.4f} {flask_sqlite_time:<30.4f} {time_delta_percent_str:<18} {flask_inmemory_mem:<30.2f} {flask_sqlite_mem:<30.2f} {mem_delta_percent_str:<18}")
    print("\n-------------------------")


if __name__ == "__main__":
    main()
