import threading
import queue
import requests
import time

# Base URL for the Flask server
BASE_URL = 'http://127.0.0.1:5000'

# Configurable number of threads and operations per thread
NUM_THREADS = 3
OPS_PER_THREAD = 100
PRINT_INTERVAL = 3  # Interval for printing intermediate results

# Queues for managing operations and latencies
operations_queue = queue.Queue()
latencies_queue = queue.Queue()

# Synchronize the starting of threads
start_event = threading.Event()

# Function to perform KV store operations and record latency
def kv_store_operation(op_type, key, value=None):
    try:
        url = f"{BASE_URL}/{key}"
        start_time = time.time()

        if op_type == 'SET':
            response = requests.post(url, json={'value': value})
        elif op_type == 'GET':
            response = requests.get(url)
        elif op_type == 'DELETE':
            response = requests.delete(url)
        else:
            raise ValueError("Invalid operation type")

        response.raise_for_status()  # This raises an error for non-2xx responses
        latency = time.time() - start_time
        latencies_queue.put(latency)
        return True
    except Exception as e:
        print(f"Error during {op_type} operation for key '{key}': {e}")
        return False

# Worker thread function to execute operations
def worker_thread():
    while not start_event.is_set():
        pass  # Wait until all threads are ready

    while not operations_queue.empty():
        op, key, value = operations_queue.get()
        kv_store_operation(op, key, value)

# Function to monitor and report performance
def monitor_performance():
    last_print = time.time()
    while True:
        time.sleep(PRINT_INTERVAL)
        current_time = time.time()
        elapsed_time = current_time - last_print
        latencies = []

        while not latencies_queue.empty():
            latencies.append(latencies_queue.get())

        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            throughput = len(latencies) / elapsed_time
            print(f"[Last {PRINT_INTERVAL} seconds] Throughput: {throughput:.2f} ops/sec, "
                  f"Avg Latency: {avg_latency:.5f} sec/ops")
        last_print = time.time()

# Function to run the benchmark for a given number of KV stores
def run_benchmark(num_kv_stores):
    # Populate the operation queue with mixed 'set' and 'get' requests
    for i in range(NUM_THREADS * OPS_PER_THREAD):
        op_type = 'SET' if i % 2 else 'GET'
        key = f"key_{i}"
        value = f"value_{i}" if op_type == 'SET' else None
        operations_queue.put((op_type, key, value))

    # Create and start worker threads
    threads = [threading.Thread(target=worker_thread) for _ in range(NUM_THREADS)]

    # Start the monitoring thread
    monitoring_thread = threading.Thread(target=monitor_performance, daemon=True)
    monitoring_thread.start()

    # Starting benchmark
    start_time = time.time()
    start_event.set()  # Signal threads to start

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # Calculate final results
    total_time = time.time() - start_time
    total_ops = NUM_THREADS * OPS_PER_THREAD
    total_latencies = list(latencies_queue.queue)
    average_latency = sum(total_latencies) / len(total_latencies) if total_latencies else float('nan')
    throughput = total_ops / total_time

    print(f"Results for {num_kv_stores} KV stores:")
    print(f"Total operations: {total_ops}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} operations per second")
    print(f"Average Latency: {average_latency:.5f} seconds per operation")

def set_num_replicas(desired_replica_count):
    response = requests.post(f"{BASE_URL}/set_replicas", json={"num_replicas": desired_replica_count})
    if response.status_code == 200:
        print(f"\nNumber of replicas set to {desired_replica_count}")
    else:
        print(f"Failed to set number of replicas. Error: {response.text}")

# Running benchmarks incrementally from 1 to 3 KV stores
if __name__ == "__main__":
    for num_kv_stores in range(1, 4):
        set_num_replicas(num_kv_stores)
        run_benchmark(num_kv_stores)