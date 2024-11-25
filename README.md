# CRYSTAL (Cryptographically Reversible Yoked Storage Layer)

*Please note that application is still in beta testing - use at your own risk. See License page before using application. *

CRYSTAL is a secure storage system ensuring data integrity and confidentiality through reversible encryption, data fragmentation, dynamic compression, and zero-knowledge verification.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
  - [Installation](#installation)
  - [Configuration](#configuration)
  - [Usage](#usage)
- [Benchmarking](#benchmarking)
- [Contributing](#contributing)
- [License](#license)
- [Final Notes](#final-notes)
- [Conclusion](#conclusion)

## Features

- **Reversible Encryption:** Enables data integrity checks and recovery without full decryption.
- **Data Fragmentation:** Splits data into encrypted fragments for distribution across nodes.
- **Dynamic Compression:** Reduces storage overhead while maintaining data fidelity.
- **Zero-Knowledge Verification:** Verifies data integrity without exposing the actual data.

## Getting Started

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yourusername/crystal.git
   cd crystal
   ```

2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Generate Encryption Keys:**

   Run the following Python script to generate and save encryption keys:

   ```python
   from crystal_storage.encryption import EncryptionManager

   manager = EncryptionManager()
   manager.serialize_keys()
   ```

### Configuration

Configure the system by editing the `config/config.yaml` file:

```yaml
fragment_size: 1024  # Size in bytes
node_paths:
  - ../data/node1
  - ../data/node2
  - ../data/node3
encryption_keys:
  private_key: ../config/private_key.pem
  public_key: ../config/public_key.pem
logging:
  level: INFO  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
```

### Usage

#### Storing Data

```bash
python scripts/store_data.py
```

#### Verifying Data Integrity

```bash
python scripts/verify_integrity.py
```

#### Recovering Data

```bash
python scripts/recover_data.py
```

### Running Benchmarks

```bash
python benchmarks/benchmark_test.py
```

## Contributing

We welcome contributions to enhance CRYSTAL! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a detailed description of your changes.
4. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.

## Final Notes

Ensure that you keep your encryption keys secure. Use version control systems responsibly, avoiding the accidental inclusion of sensitive data.

## Conclusion

CRYSTAL provides an innovative, secure, and efficient way to manage and safeguard data. We look forward to your feedback and contributions!
