import asyncio
import os
import logging
import hashlib
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
import aiofiles
import zlib
import random

# Configure logging
LOG_LEVEL = logging.INFO  # Set to logging.DEBUG for detailed logs
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")


class KeyManagementSystem:
    """Deterministic Key Management System using dataset checksum."""
    def __init__(self, dataset_checksum):
        self.dataset_checksum = dataset_checksum

    def generate_key(self, fragment_id, replica=0):
        """Generate deterministic key and nonce for a fragment and its replica."""
        # Include replica index to generate different keys for replicas
        seed = f"{fragment_id}:{self.dataset_checksum}:{replica}".encode()
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=seed[:16],
            iterations=100000,
            backend=default_backend()
        )
        key = kdf.derive(seed)
        nonce = seed[:12]  # GCM requires a 12-byte nonce
        return key, nonce


def calculate_checksum(data):
    """Calculate a SHA-256 checksum."""
    return hashlib.sha256(data).digest()


def dynamic_concurrency(dataset_size, fragment_size):
    """Determine max concurrent tasks based on dataset size and fragment size."""
    cpu_count = multiprocessing.cpu_count()
    base_concurrency = min(50, cpu_count * 2)  # Base level concurrency

    # Scale with dataset size
    if dataset_size <= 1024 * 100:  # Small datasets (<= 100 KB)
        return max(5, base_concurrency // 4)  # Further reduce concurrency for very small datasets
    elif dataset_size <= 1024 * 10000:  # Medium datasets (<= 10 MB)
        return base_concurrency
    else:  # Large datasets (> 10 MB)
        return min(300, base_concurrency * 3)  # Maximize concurrency for large datasets


def optimal_fragment_size(dataset_size):
    """Determine optimal fragment size based on dataset size."""
    if dataset_size <= 1024 * 100:  # <= 100 KB
        return dataset_size  # Single fragment
    elif dataset_size <= 1024 * 1000:  # <= 1 MB
        return 1024 * 50  # 50 KB
    elif dataset_size <= 1024 * 10000:  # <= 10 MB
        return 1024 * 100  # 100 KB
    else:  # > 10 MB
        return 1024 * 200  # 200 KB


def encrypt_fragment_sync(args):
    """Synchronous encryption function for ProcessPoolExecutor."""
    fragment_id, fragment, dataset_checksum, replica = args
    kms = KeyManagementSystem(dataset_checksum)
    key, nonce = kms.generate_key(fragment_id, replica)
    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_fragment = encryptor.update(fragment) + encryptor.finalize()
    tag = encryptor.tag
    checksum = calculate_checksum(encrypted_fragment)
    # Removed logging to prevent pickling issues
    return (fragment_id, replica, encrypted_fragment, tag, checksum)


def compress_data(data):
    """Compress data using zlib."""
    return zlib.compress(data)


def decompress_data(data):
    """Decompress data using zlib."""
    return zlib.decompress(data)


class AssemblyLine:
    def __init__(self, node_paths, dataset_checksum, fragment_size, replication_factor=3):
        self.node_paths = node_paths
        self.kms = KeyManagementSystem(dataset_checksum)
        self.fragment_size = fragment_size
        self.replication_factor = replication_factor
        # Initialize ProcessPoolExecutor for CPU-bound tasks
        self.executor = ProcessPoolExecutor(max_workers=multiprocessing.cpu_count())

    async def fragment_data(self, data, fragment_size=None):
        """Split data into fragments."""
        if fragment_size is None:
            fragment_size = self.fragment_size
        fragments = [data[i:i + fragment_size] for i in range(0, len(data), fragment_size)]
        logging.info(f"Fragmentation complete: {len(fragments)} fragments created.")
        return fragments

    async def inline_process_small_dataset(self, data):
        """Process small datasets inline to minimize overhead."""
        fragment_id = 0
        compressed_data = compress_data(data)
        encrypted_fragment, tag, checksum = self.encrypt_fragment_sync_inline(fragment_id, compressed_data, replica=0)
        return [(fragment_id, 0, encrypted_fragment, tag, checksum)]

    def encrypt_fragment_sync_inline(self, fragment_id, compressed_fragment, replica=0):
        """Inline encryption for small datasets."""
        key, nonce = self.kms.generate_key(fragment_id, replica)
        cipher = Cipher(algorithms.AES(key), modes.GCM(nonce), backend=default_backend())
        encryptor = cipher.encryptor()
        encrypted_fragment = encryptor.update(compressed_fragment) + encryptor.finalize()
        tag = encryptor.tag
        checksum = calculate_checksum(encrypted_fragment)
        return encrypted_fragment, tag, checksum

    async def encrypt_fragments(self, fragments, inline=False):
        """Encrypt fragments using AES-GCM, parallelized."""
        if inline and len(fragments) == 1:
            # Inline processing for small datasets
            compressed_data = compress_data(fragments[0])
            encrypted_fragment, tag, checksum = self.encrypt_fragment_sync_inline(0, compressed_data, replica=0)
            return [(0, 0, encrypted_fragment, tag, checksum)]

        loop = asyncio.get_event_loop()
        # Prepare args: (fragment_id, fragment, dataset_checksum, replica)
        # For replication, prepare multiple args per fragment
        args = []
        for i, fragment in enumerate(fragments):
            compressed_fragment = compress_data(fragment)
            for replica in range(self.replication_factor):
                args.append((i, compressed_fragment, self.kms.dataset_checksum, replica))
        tasks = [
            loop.run_in_executor(self.executor, encrypt_fragment_sync, arg)
            for arg in args
        ]
        encrypted_fragments = await asyncio.gather(*tasks)
        logging.info("Encryption complete.")
        return encrypted_fragments

    async def store_fragments(self, encrypted_fragments, max_concurrent_tasks=100):
        """Store encrypted fragments asynchronously with batching."""
        semaphore = asyncio.Semaphore(max_concurrent_tasks)
        # Adjust batch size based on total fragments
        total_fragments = len(encrypted_fragments) // self.replication_factor
        if total_fragments > 1000:
            batch_size = 50
        elif total_fragments > 500:
            batch_size = 25
        else:
            batch_size = 10

        async def store_batch(batch):
            tasks = [
                self._store_fragment(fragment_id, replica, encrypted_fragment, tag, checksum)
                for fragment_id, replica, encrypted_fragment, tag, checksum in batch
            ]
            await asyncio.gather(*tasks)

        for i in range(0, len(encrypted_fragments), batch_size * self.replication_factor):
            batch = encrypted_fragments[i:i + batch_size * self.replication_factor]
            async with semaphore:
                await store_batch(batch)
        logging.info("All fragments stored.")

    async def _store_fragment(self, fragment_id, replica, encrypted_fragment, tag, checksum):
        node_index = (fragment_id * self.replication_factor + replica) % len(self.node_paths)
        node_path = self.node_paths[node_index]
        fragment_path = os.path.join(node_path, f"fragment_{fragment_id}_replica_{replica}.pkl")
        async with aiofiles.open(fragment_path, "wb") as f:
            # Store encrypted_fragment + tag + checksum
            await f.write(encrypted_fragment + tag + checksum)
        logging.debug(f"Fragment {fragment_id} replica {replica} stored at {fragment_path}.")

    async def retrieve_fragments(self, num_fragments, max_concurrent_tasks=100):
        """Retrieve and decrypt fragments asynchronously with replication."""
        semaphore = asyncio.Semaphore(max_concurrent_tasks)
        # Adjust batch size based on total fragments
        if num_fragments > 1000:
            batch_size = 50
        elif num_fragments > 500:
            batch_size = 25
        else:
            batch_size = 10

        # For each fragment, list of replicas
        fragment_replicas = {}
        for fragment_id in range(num_fragments):
            replicas = list(range(self.replication_factor))
            fragment_replicas[fragment_id] = replicas

        # Define retrieval tasks per fragment
        async def retrieve_fragment(fragment_id):
            replicas = fragment_replicas[fragment_id]
            for replica in replicas:
                node_index = (fragment_id * self.replication_factor + replica) % len(self.node_paths)
                node_path = self.node_paths[node_index]
                fragment_path = os.path.join(node_path, f"fragment_{fragment_id}_replica_{replica}.pkl")

                if not os.path.exists(fragment_path):
                    logging.warning(f"Fragment {fragment_id} replica {replica} is missing.")
                    continue

                async with aiofiles.open(fragment_path, "rb") as f:
                    data = await f.read()

                if len(data) < 48:  # encrypted_fragment + tag + checksum
                    logging.error(f"Fragment {fragment_id} replica {replica} data is incomplete or corrupted.")
                    continue

                encrypted_fragment = data[:-48]
                tag = data[-48:-32]
                checksum = data[-32:]

                # Verify checksum
                calculated_checksum = calculate_checksum(encrypted_fragment)
                if checksum != calculated_checksum:
                    logging.error(f"Checksum mismatch for fragment {fragment_id} replica {replica}.")
                    continue

                # Decrypt
                key, nonce = self.kms.generate_key(fragment_id, replica)
                cipher = Cipher(algorithms.AES(key), modes.GCM(nonce, tag), backend=default_backend())
                decryptor = cipher.decryptor()

                try:
                    decrypted_compressed_fragment = decryptor.update(encrypted_fragment) + decryptor.finalize()
                    decrypted_fragment = decompress_data(decrypted_compressed_fragment)
                    logging.debug(f"Fragment {fragment_id} replica {replica}: Decrypted and decompressed data.")
                    return decrypted_fragment
                except Exception as e:
                    logging.error(f"Decryption failed for fragment {fragment_id} replica {replica}: {e}")
                    continue

            # If all replicas failed
            logging.error(f"All replicas failed for fragment {fragment_id}.")
            return None

        # Batch retrieval
        fragments = []
        for i in range(0, num_fragments, batch_size):
            batch_ids = range(i, min(i + batch_size, num_fragments))
            async with semaphore:
                retrieved = await asyncio.gather(*[retrieve_fragment(fid) for fid in batch_ids])
                for fid, frag in zip(batch_ids, retrieved):
                    if frag is not None:
                        fragments.append((fid, frag))
        fragments = [f for f in fragments if f is not None]
        logging.info(f"Retrieved {len(fragments)}/{num_fragments} fragments.")
        return fragments

    async def parallel_reassemble(self, fragments):
        """Parallelize the reassembly process for large datasets."""
        if not fragments:
            logging.error("No fragments available for reassembly.")
            return b""

        # Sort fragments by fragment_id
        fragments_sorted = sorted(fragments, key=lambda x: x[0])
        data_chunks = [fragment[1] for fragment in fragments_sorted]

        # Concatenate all data chunks
        return b''.join(data_chunks)
