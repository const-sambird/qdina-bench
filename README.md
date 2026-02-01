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

### venv creation

Then, create a venv and install the Python prerequisites:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### TPC runkits

Then, download the runkits from the TPC website and create a Makefile by renaming and editing makefile.suite to your system specifications. qgen will not compile on macOS without changing references from malloc.h to stdlib.h. (The experimental results for qDINA were run on Ubuntu 24.04, and a Linux environment is recommended for reproducibility).

**This benchmark assumes the TPC-H kit is saved to `./tpc-h/`.**

## Benchmarking

### Creating a query set

This is not a strict requirement of running the benchmark in general, but for benchmarking qDINA, we use the same query workload for recommendation as evaluation. If the TPC-H utilities have been downloaded and the Makefile configured, [qgen.py](./qgen.py) will create a uniformly distributed query set for use.

```
usage: qgen.py [-h] [-s SCALE_FACTOR] [-n QUERIES_PER_TEMPLATE] [-o OUT_PATH]

options:
  -h, --help            show this help message and exit
  -s SCALE_FACTOR, --scale-factor SCALE_FACTOR
                        tpc-h scale factor for generated queries
  -n QUERIES_PER_TEMPLATE, --queries-per-template QUERIES_PER_TEMPLATE
                        queries to generate from each template
  -o OUT_PATH, --out-path OUT_PATH
                        location to write generated queries
```

### Specify the configuration

#### Replicas

qDINA benchmarker requires a `replicas.csv` file to list the database replicas to create (simulated) indexes on. The format that is expected for a single connection is

```
id,hostname,port,dbname,user,password,
```

| Field     | Explanation
|-----------|------------------------------------
| id        | A number to identify the database replica (1, 2, ...)
| hostname  | The IP address of the PostgreSQL database
| port      | Which port number to connect to (the default is 5432 but it must be specified)
| user      | The user to connect with. This user must have sufficient privileges on the database to create and drop hypothetical indexes and run EXPLAIN commands
| password  | The password for the user

One line per replica.

#### Index candidates

A divergent design creates different indexes on each replica. Create a file `config.csv` and add the indexes, one per line, in the format `replica,column,column,[...],column`. For example:

```
0,l_orderkey
0,r_regionkey,r_nationkey
1,o_ordername
```

#### Routing table

We need to know where to route each query type in thr workload. Create `routes.csv` and specify the index of each replica to send each query template to. Example:

```
1,0,1,0,1,1,0,1,0,0,0,0,1,0,1,1,1,1,0,1,1,1
```

### Running the benchmark

We benchmarked qDINA with the TPC-H benchmark at scale factor 10. To reproduce our results, run the following command after appropriately configuring:

```
python run.py -s 10 -v -c --copy-source [/path/to/query/workload] h all
```
