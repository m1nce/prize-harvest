import argparse
from queue import Queue
from threading import Thread
from tqdm import tqdm
from get_dates import fetch_and_store_data
from process import worker, reprocess_error_dates, error_dates
from database import close_connection

# Function to parse command-line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="NBA Data Loader")
    parser.add_argument('--start_year', type=int, required=True, help='The start year of the date range')
    parser.add_argument('--end_year', type=int, required=True, help='The end year of the date range')
    parser.add_argument('--num_workers', type=int, default=4, help='The number of worker threads')
    return parser.parse_args()

def main():
    args = parse_args()

    # Create a queue and add dates
    print('Getting dates...')
    flattened_dates = fetch_and_store_data(range(args.start_year, args.end_year + 1))
    queue = Queue()
    for date in flattened_dates:
        queue.put(date)

    # Create and start threads
    num_workers = args.num_workers
    threads = []
    with tqdm(total=len(flattened_dates)) as pbar:
        for _ in range(num_workers):  # Number of worker threads
            t = Thread(target=worker, args=(queue, pbar, num_workers))
            t.start()
            threads.append(t)

        # Wait for all tasks in the queue to be processed
        queue.join()

        # Wait for all threads to finish
        for t in threads:
            t.join()

    # Reprocess dates that encountered errors
    if error_dates:
        print(f"Reprocessing {len(error_dates)} error dates...")
        reprocess_error_dates(num_workers)

    # Close the connection
    close_connection()

    print("------------------------------------")
    print("Done!")

if __name__ == '__main__':
    main()
