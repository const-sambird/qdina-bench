# qdina-bench

This repository provides a way to benchmark [qDINA](https://github.com/const-sambird/dina), or indeed any other divergent index recommendation algorithm.

> [!NOTE]
> This benchmark is merely a comparison of query execution times and not an actual implementation of the TPC-H benchmark (which reports query-per-hour performance, power, and throughput metrics). For an implementation of TPC-H for divergent indexing, see [this repository](https://github.com/const-sambird/tpch-psql).

This benchmark implements three phases, any or all of which may be run:

- Generation of TPC-H/DS table data and queries
- Loading of generated table data into PostgreSQL databases
- Execution of the benchmark, which creates recommended secondary indexes and then measures query execution times

## Installation

### Prerequisites

This benchmark is built on Python 3.12, though other versions may be compatible. Download the desired runkit(s) from the [TPC website](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp). TPC-H requires `gcc`, while TPC-DS requires more packages:

```bash
sudo apt-get install gcc make flex bison byacc git gcc-9
```

To run the benchmark, a cluster of PostgreSQL databases must be instantiated and connection permissions configured, etc. This benchmark was written for Postgres 17.
