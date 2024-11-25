import asyncio
import os
import time
import multiprocessing
import psutil
import matplotlib.pyplot as plt
import random
import logging
import aiofiles
from pipeline import AssemblyLine, calculate_checksum, dynamic_concurrency, optimal_fragment_size

# Configure logging for benchmark
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define node paths
NODE_PATHS = ['data/node1', 'data/node2', 'data/node3']
for path in NODE_PATHS:
    os.makedirs(path, exist_ok=True)


def log_resource_usage(stage):
    """Log CPU and memory usage at key stages."""
    cpu_percent = psutil.cpu_percent(interval=0.5)
    memory = psutil.virtual_memory()
    memory_used = memory.used / (1024 ** 2)  # Convert to MB
    memory_total = memory.total / (1024 ** 2)  # Convert to MB
    print(f"[{stage}] CPU Usage: {cpu_percent}% | Memory Usage: {memory_used:.2f} MB / {memory_total:.2f} MB")


async def simulate_node_failure(node_paths, failure_rate=0.05):
    """Randomly delete or corrupt some fragments to simulate node failure."""
    for node_path in node_paths:
        files = os.listdir(node_path)
        for file in files:
            if random.random() < failure_rate:
                fragment_path = os.path.join(node_path, file)
                if random.random() < 0.5:
                    # Delete the fragment to simulate missing data
                    os.remove(fragment_path)
                    logging.warning(f"Simulated failure: Deleted fragment {fragment_path}")
                else:
                    # Corrupt the fragment by altering its contents
                    async with aiofiles.open(fragment_path, "rb+") as f:
                        data = await f.read()
                        if len(data) >= 10:
                            corrupted_data = bytearray(data)
                            corrupted_data[10] ^= 0xFF  # Flip a bit
                            await f.seek(0)
                            await f.write(corrupted_data)
                            logging.warning(f"Simulated failure: Corrupted fragment {fragment_path}")


async def benchmark():
    # Define dataset sizes in KB
    DATA_SIZES = [10, 100, 1000, 10000, 100000, 1000000]  # 10 KB to 1 GB

    # Convert KB to bytes for processing
    DATA_SIZES_BYTES = [size * 1024 for size in DATA_SIZES]

    results = []

    for data_size, data_size_kb in zip(DATA_SIZES_BYTES, DATA_SIZES):
        print(f"\n--- Starting Benchmark for Dataset Size: {data_size_kb:.2f} KB ---")

        # Clean up previous fragments
        for node_path in NODE_PATHS:
            for file in os.listdir(node_path):
                file_path = os.path.join(node_path, file)
                os.remove(file_path)

        # Determine optimal fragment size
        fragment_size = optimal_fragment_size(data_size)
        print(f"Using Fragment Size: {fragment_size / 1024:.2f} KB")

        # Generate random data
        if data_size > 1024 * 1024 * 500:  # If dataset > 500 MB, generate in chunks
            data = bytearray()
            chunk_size = 1024 * 1024  # 1 MB
            for _ in range(data_size // chunk_size):
                data += os.urandom(chunk_size)
            remaining = data_size % chunk_size
            if remaining:
                data += os.urandom(remaining)
            data = bytes(data)
        else:
            data = os.urandom(data_size)
        dataset_checksum = calculate_checksum(data)
        # Set replication factor based on dataset size
        if data_size > 1024 * 10000:  # > 10 MB
            replication_factor = 3
        else:
            replication_factor = 2
        assembly_line = AssemblyLine(NODE_PATHS, dataset_checksum, fragment_size, replication_factor=replication_factor)

        # Determine dynamic concurrency
        max_concurrent_tasks = dynamic_concurrency(data_size, fragment_size)
        print(f"Using Max Concurrent Tasks: {max_concurrent_tasks}")

        start_time = time.time()

        # Fragmentation
        print("Starting fragment creation...")
        log_resource_usage("Fragment Creation")
        fragments = await assembly_line.fragment_data(data, fragment_size)
        print(f"Fragments created: {len(fragments)}")

        # Determine if dataset is small and should be processed inline
        inline = False
        if data_size <= 1024 * 100:  # <= 100 KB
            inline = True

        # Encryption
        print("Starting encryption...")
        log_resource_usage("Encryption")
        encrypted_fragments = await assembly_line.encrypt_fragments(fragments, inline=inline)
        print("Encryption completed.")

        # Batched Storage
        print("Starting batched storage...")
        log_resource_usage("Batched Storage")
        await assembly_line.store_fragments(encrypted_fragments, max_concurrent_tasks=max_concurrent_tasks)
        print("Storage completed.")

        # Simulate Node Failures for testing (e.g., 5% failure rate)
        if data_size >= 1024 * 1000:  # Only simulate failures for datasets >= 1 MB
            print("Simulating node failures...")
            await simulate_node_failure(NODE_PATHS, failure_rate=0.05)  # 5% failure rate

        # Retrieval
        print("Starting retrieval...")
        log_resource_usage("Retrieval")
        retrieved_fragments = await assembly_line.retrieve_fragments(len(fragments), max_concurrent_tasks=max_concurrent_tasks)
        print(f"Retrieved fragments: {len(retrieved_fragments)}")

        # Reassembly
        print("Reassembling data...")
        log_resource_usage("Data Reassembly")
        recovered_data = await assembly_line.parallel_reassemble(retrieved_fragments)
        print("Data reassembly completed.")

        # Verification
        elapsed_time = time.time() - start_time
        is_verified = recovered_data == data
        print(f"Data verification: {'Success' if is_verified else 'Failure'}")

        # Append results
        results.append({
            "Dataset Size (KB)": data_size_kb,
            "Fragment Size (KB)": fragment_size / 1024,
            "Time (s)": round(elapsed_time, 6),
            "Integrity Verified": "Success" if is_verified else "Failure",
            "Max Concurrent Tasks": max_concurrent_tasks
        })

    # Summary
    print("\n==== Summary ====")
    header = f"{'Dataset Size (KB)':<20}{'Fragment Size (KB)':<20}{'Time (s)':<12}{'Integrity Verified':<20}{'Max Concurrency':<20}"
    print(header)
    print("-" * len(header))
    for result in results:
        print(f"{result['Dataset Size (KB)']:<20}{result['Fragment Size (KB)']:<20}{result['Time (s)']:<12}{result['Integrity Verified']:<20}{result['Max Concurrent Tasks']:<20}")

    # Visualization
    dataset_sizes_kb = [result["Dataset Size (KB)"] for result in results]
    times = [result["Time (s)"] for result in results]

    plt.figure(figsize=(12, 8))
    plt.plot(dataset_sizes_kb, times, marker='o', linestyle='-', color='b', label='Processing Time')
    plt.title('CRYSTAL Benchmark: Dataset Size vs. Processing Time')
    plt.xlabel('Dataset Size (KB)')
    plt.ylabel('Time (s)')
    plt.xscale('log')  # Log scale for better visualization
    plt.yscale('log')  # Log scale to accommodate wide range of times
    plt.grid(True, which="both", ls="--", linewidth=0.5)
    plt.legend()
    plt.savefig('optimized_benchmark_results.png')
    plt.show()


if __name__ == "__main__":
    # Set multiprocessing start method to 'spawn' to avoid pickling issues with 'fork'
    multiprocessing.set_start_method('spawn', force=True)
    asyncio.run(benchmark())
