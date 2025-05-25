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

# Fixed parameters for data generation
DATA_GEN_START_DATE = "2023-01-01"
DATA_GEN_END_DATE = "2023-03-31" # 3 months of data
DATA_GEN_NUM_NICKS = 50
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
        "query_args": {"s": "user", "nick": "User1"} # "user" is common, User1 will exist
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
        "name": "search_day_logs_simple",
        "method_to_call": "search_day_logs",
        "query_args": {"s": "database"} # "database" is in VOCABULARY
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
        "--num-nicks", str(DATA_GEN_NUM_NICKS),
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
    parser.add_argument("--dataset-sizes", type=str, default="10000,100000",
                        help="Comma-separated string of log entry counts (e.g., '10000,100000').")
    parser.add_argument("--output-results-file", type=str, default="benchmark_results.json",
                        help="File to save benchmark results.")
    parser.add_argument("--data-gen-script", type=str, default="tools/generate_log_data.py",
                        help="Path to the data generation script.")
    parser.add_argument("--temp-data-file-prefix", type=str, default="temp_benchmark_data",
                        help="Prefix for temporary log data files.")
    parser.add_argument("--runs-per-test", type=int, default=3,
                        help="Number of times to run each specific benchmark test for averaging.")

    args = parser.parse_args()

    app.testing = True # Set Flask app to testing mode

    dataset_sizes = [int(s.strip()) for s in args.dataset_sizes.split(',')]
    engine_classes = {
        "InMemory": InMemoryLogQueryEngine,
        "SQLite": SQLiteLogQueryEngine
    }
    
    all_run_results = [] # Store results from every single run

    base_seed = random.randint(1, 10000) # Use a base seed to make multiple script executions somewhat varied but internally consistent

    for size_idx, size in enumerate(dataset_sizes):
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

                    all_run_results.append({
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
                            
                            # Clear caches for InMemory engine before each query test
                            if isinstance(current_engine, InMemoryLogQueryEngine):
                                current_engine.clear_all_caches()
                            
                            try:
                                q_time, q_peak_mem = run_single_test(current_engine, query_def["method_to_call"], query_def["query_args"])
                                all_run_results.append({
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

        finally:
            if os.path.exists(temp_data_file):
                print(f"Cleaning up temporary data file: {temp_data_file}")
                try:
                    os.remove(temp_data_file)
                except OSError as e:
                    print(f"Error removing temporary file {temp_data_file}: {e}", file=sys.stderr)
    
    # Aggregate results
    print("\nAggregating results...")
    aggregated_results = {}
    for record in all_run_results:
        key = (record["engine"], record["dataset_size"], record["operation"])
        if key not in aggregated_results:
            aggregated_results[key] = {"times": [], "mems": []}
        aggregated_results[key]["times"].append(record["time_seconds"])
        aggregated_results[key]["mems"].append(record["peak_memory_bytes"])

    final_results_summary = []
    for key, data in aggregated_results.items():
        avg_time = sum(data["times"]) / len(data["times"])
        avg_mem = sum(data["mems"]) / len(data["mems"])
        final_results_summary.append({
            "engine": key[0],
            "dataset_size": key[1],
            "operation": key[2],
            "avg_time_seconds": avg_time,
            "avg_peak_memory_bytes": avg_mem,
            "runs": len(data["times"])
        })
    
    # Sort final results for consistent output
    final_results_summary.sort(key=lambda x: (x["dataset_size"], x["engine"], x["operation"]))

    # Write aggregated results to file
    try:
        with open(args.output_results_file, 'w') as f:
            json.dump(final_results_summary, f, indent=2)
        print(f"Benchmark results saved to {args.output_results_file}")
    except IOError as e:
        print(f"Error writing results to file: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
