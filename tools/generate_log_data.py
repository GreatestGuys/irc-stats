import argparse
import datetime
import json
import random
import time
import string

VOCABULARY = [
    "error", "warning", "info", "debug", "request", "response", "user", "login", 
    "logout", "payment", "file", "database", "success", "failure", "system",
    "network", "service", "module", "component", "process", "event", "data",
    "critical", "timeout", "exception", "connection", "authentication", "authorization",
    "update", "query", "server", "client", "api", "version", "status", "initiation"
]

# Words that might appear more frequently
COMMON_WORDS = ["request", "response", "info", "debug", "error", "user"]


def generate_random_timestamp(start_dt, end_dt):
    """Generates a random timestamp between two datetime objects."""
    start_ts = time.mktime(start_dt.timetuple())
    end_ts = time.mktime(end_dt.timetuple())
    return random.uniform(start_ts, end_ts)

def generate_random_message(vocab, common_words):
    """Generates a random log message."""
    message_len = random.randint(3, 10)
    message_parts = []
    for _ in range(message_len):
        # Increase probability of common words
        if random.random() < 0.4 and common_words: # 40% chance to pick from common_words
            message_parts.append(random.choice(common_words))
        else:
            message_parts.append(random.choice(vocab))
    
    # Occasionally add a random number
    if random.random() < 0.2: # 20% chance
        message_parts.append(str(random.randint(100, 9999)))
    
    # Occasionally add a short random string
    if random.random() < 0.1: # 10% chance
        random_str_len = random.randint(3, 6)
        random_chars = ''.join(random.choices(string.ascii_lowercase + string.digits, k=random_str_len))
        message_parts.append(random_chars)
        
    random.shuffle(message_parts) # Shuffle to make it less predictable
    return ' '.join(message_parts)

def main():
    parser = argparse.ArgumentParser(description="Generate large JSON log datasets.")
    parser.add_argument("--num-entries", type=int, required=True, help="Total number of log entries to generate.")
    parser.add_argument("--start-date", type=str, required=True, help="Start date for log entries (YYYY-MM-DD).")
    parser.add_argument("--end-date", type=str, required=True, help="End date for log entries (YYYY-MM-DD).")
    parser.add_argument("--num-nicks", type=int, default=10, help="Number of unique nicks to generate (default: 10).")
    parser.add_argument("--output-file", type=str, default="generated_log_data.json", help="Path to save the generated JSON data (default: generated_log_data.json).")
    parser.add_argument("--seed", type=int, help="Seed for the random number generator.")
    parser.add_argument("--pretty-print", action='store_true', default=False, help="If set, pretty-print the JSON output.")

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    # Generate nicks
    nicks = [f"User{i+1}" for i in range(args.num_nicks)]

    # Parse dates
    try:
        start_dt = datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
        end_dt = datetime.datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError as e:
        print(f"Error parsing date: {e}")
        return

    if start_dt >= end_dt:
        print("Error: Start date must be before end date.")
        return

    log_entries = []
    print(f"Generating {args.num_entries} log entries...")

    for i in range(args.num_entries):
        timestamp_float = generate_random_timestamp(start_dt, end_dt)
        selected_nick = random.choice(nicks)
        message = generate_random_message(VOCABULARY, COMMON_WORDS)
        
        log_entry = {
            "timestamp": str(timestamp_float), # Store as string, like original format
            "nick": selected_nick,
            "message": message
        }
        log_entries.append(log_entry)
        
        if (i + 1) % (args.num_entries // 10 if args.num_entries >= 10 else 1) == 0 :
             print(f"Generated {i+1}/{args.num_entries} entries...")


    # Sort log entries by timestamp
    print("Sorting log entries by timestamp...")
    log_entries.sort(key=lambda x: float(x["timestamp"]))

    # Write to output file
    print(f"Writing log entries to {args.output_file}...")
    try:
        with open(args.output_file, 'w') as f:
            if args.pretty_print:
                json.dump(log_entries, f, indent=2)
            else:
                json.dump(log_entries, f)
        print(f"Successfully generated {len(log_entries)} log entries to {args.output_file}")
    except IOError as e:
        print(f"Error writing to file: {e}")

if __name__ == "__main__":
    main()
