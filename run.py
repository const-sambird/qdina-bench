import argparse
import logging
import os

from benchmark import Benchmark
from replica import Replica
from tpch_generator import TPCHGenerator
from tpcds_generator import TPCDSGenerator
from query_loader import load_test_set_queries

def create_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--scale-factor', type=int, default=10, help='the TPC-H/DS scale factor')
    parser.add_argument('-g', '--dbgen-dir', type=str, default=None, help='the path to the TPC-H tools dbgen directory')
    parser.add_argument('-d', '--data-dir', type=str, default='./data', help='the path where the data generated should be stored')
    parser.add_argument('-r', '--replicas', type=str, default='replicas.csv', help='the CSV file with replica connection details')
    parser.add_argument('-i', '--index-config', type=str, default='config.csv', help='the path to the index configuration')
    parser.add_argument('-t', '--routing-table', type=str, default='routes.csv', help='the path to the routing table')
    parser.add_argument('-p', '--partial-templates', type=str, default='partial.csv', help='the templates used in the training partition (can be empty/nonexistent)')
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose log output')
    parser.add_argument('-c', '--copy-test-set', action='store_true', help='use pregenerated queries from an existing test set instead of generated queries')
    parser.add_argument('-e', '--rng-seed', type=int, default=None, help='seed to pass to the database generator')
    parser.add_argument('--copy-source', type=str, default='/proj/qdina-PG0/dina-set/h/test', help='where the test set is stored')
    
    parser.add_argument('benchmark', choices=['h', 'ds'], help='which TPC benchmark should be run? TPC-[H] or TPC-[DS]?')
    parser.add_argument('phase', choices=['generate', 'load', 'run', 'all'], nargs='+', help='which phases of the benchmark should be run? if all is present, run all.')

    return parser.parse_args()

def tpch_table_from_column_prefix(column: str) -> str:
    '''
    Given the name of a column in the TPC-H benchmark,
    returns the name of the table it is on based on the
    prefix attached to the column name.

    :param column: the column name (eg ps_suppkey)
    :returns: the table name (eg PARTSUPP)
    '''
    PREFIXES = {
        'l': 'LINEITEM',
        'p': 'PART',
        'ps': 'PARTSUPP',
        'o': 'ORDERS',
        'c': 'CUSTOMER',
        'n': 'NATION',
        'r': 'REGION',
        's': 'SUPPLIER'
    }
    prefix = column.split('_')[0]

    return PREFIXES[prefix]

def tpcds_table_from_column_prefix(column: str) -> str:
    '''
    Given the name of a column in the TPC-H benchmark,
    returns the name of the table it is on based on the
    prefix attached to the column name.

    :param column: the column name (eg ps_suppkey)
    :returns: the table name (eg PARTSUPP)
    '''
    PREFIXES = {
        'ss': 'STORE_SALES',
        'sr': 'STORE_RETURNS',
        'cs': 'CATALOG_SALES',
        'cr': 'CATALOG_RETURNS',
        'ws': 'WEB_SALES',
        'wr': 'WEB_RETURNS',
        'inv': 'INVENTORY',
        's': 'STORE',
        'cc': 'CALL_CENTER',
        'cp': 'CATALOG_PAGE',
        'web': 'WEB_SITE',
        'wp': 'WEB_PAGE',
        'w': 'WAREHOUSE',
        'c': 'CUSTOMER',
        'ca': 'CUSTOMER_ADDRESS',
        'cd': 'CUSTOMER_DEMOGRAPHICS',
        'd': 'DATE_DIM',
        'hd': 'HOUSEHOLD_DEMOGRAPHICS',
        'i': 'ITEM',
        'ib': 'INCOME_BAND',
        'p': 'PROMOTION',
        'r': 'REASON',
        'sm': 'SHIP_MODE',
        't': 'TIME_DIM',
        'dv': 'DSDGEN_VERSION'        
    }
    prefix = column.split('_')[0]

    return PREFIXES[prefix]

def get_replicas(path: str, benchmark: str):
    replicas = []
    with open(path, 'r') as infile:
        lines = infile.readlines()
        for config in lines:
            fields = config.split(',')
            replicas.append(
                Replica(
                    id=fields[0],
                    hostname=fields[1],
                    port=fields[2],
                    dbname=fields[3],
                    user=fields[4],
                    password=fields[5]
                )
            )
    return replicas

def get_index_config(path: str, num_replicas: int, benchmark: str) -> list[tuple[str]]:
    indexes = []
    for replica in range(num_replicas):
        indexes.append([])
    
    with open(path, 'r') as infile:
        lines = infile.readlines()
        for index in lines:
            fields = index.split(',')
            to_replica = int(fields[0])
            if benchmark == 'h':
                table = tpch_table_from_column_prefix(fields[1])
            else:
                table = tpcds_table_from_column_prefix(fields[1])
            indexes[to_replica].append([table, fields[1:]])
    
    return indexes

def get_routes(path: str) -> list[int]:
    routes = None

    with open(path, 'r') as infile:
        table = infile.readline()
        routes = table.split(',')
        routes = [int(r) for r in routes]
    
    return routes

def get_partial_templates(path: str) -> list[int]:
    templates = None

    if not os.path.isfile(path):
        return []
    
    with open(path, 'r') as infile:
        table = infile.readline()
        templates = table.split(',')
        templates = [int(t) - 1 for t in templates]

    return templates

if __name__ == '__main__':
    args = create_arguments()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
    
    PHASES_TO_RUN = args.phase
    DBGEN_DIR = args.dbgen_dir
    COPY_TEST_SET = args.copy_test_set
    COPY_SOURCE = args.copy_source
    RNG_SEED = args.rng_seed

    if 'all' in args.phase:
        PHASES_TO_RUN = ['generate', 'load', 'run']

    if DBGEN_DIR is None:
        if args.benchmark == 'h':
            DBGEN_DIR = './tpc-h/dbgen'
        else:
            DBGEN_DIR = './tpc-ds/tools'
    
    replicas = get_replicas(args.replicas, args.benchmark)
    config = get_index_config(args.index_config, len(replicas), args.benchmark)
    routes = get_routes(args.routing_table)
    partial_temps = get_partial_templates(args.partial_templates)

    base = os.path.dirname(os.path.realpath(__file__))
    data_dir = os.path.join(base, args.data_dir)
    data_dir = os.path.normpath(data_dir)
    dbgen_dir = os.path.join(base, DBGEN_DIR)
    dbgen_dir = os.path.normpath(dbgen_dir)

    if args.benchmark == 'h':
        generator = TPCHGenerator(replicas, dbgen_dir, data_dir, args.scale_factor)
    else:
        generator = TPCDSGenerator(replicas, dbgen_dir, data_dir, args.scale_factor)

    args.benchmark = args.benchmark.upper()

    if 'generate' in PHASES_TO_RUN:
        logging.info(f'generating TPC-{args.benchmark} data, scale factor {args.scale_factor}')
        generator.generate(RNG_SEED)
    else:
        logging.info(f'skipping TPC-{args.benchmark} data generation')

    if 'load' in PHASES_TO_RUN:
        logging.info(f'loading TPC-{args.benchmark} data')
        generator.load_database()
    else:
        logging.info(f'skipping TPC-{args.benchmark} database load. it must already be present in the database!')

    if 'run' in PHASES_TO_RUN:
        if COPY_TEST_SET:
            queries, templates = load_test_set_queries(COPY_SOURCE)
        else:
            queries, templates = generator.read_data()
        benchmark = Benchmark(queries, templates, replicas, routes, config)

        total, times = benchmark.run()

        partial = 0
        for i in partial_temps:
            partial += times[i]

        logging.info('=' * 30)
        logging.info(f'TPC-{args.benchmark} Performance Benchmark Results')
        logging.info('')
        logging.info(f'Total Runtime                = {round(total, 3)}')
        if len(partial_temps) > 0:
            logging.info(f'Training Partition Runtime   = {round(partial, 3)}')
        logging.info('')
        for i, time in enumerate(times):
            logging.info(f'Q{i + 1}                     = {round(time, 3)}')
        logging.info('')
        logging.info(f'Scale factor: {args.scale_factor}')
        logging.info('=' * 30)
