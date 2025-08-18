import glob
import os
import subprocess
import shutil
import logging
import random

from replica import Replica
from connection import Connection
from generator import Generator

TABLE_BLOCK_SIZE = 5_120_000

class TPCDSGenerator(Generator):
    def __init__(self, replicas: list[Replica], dbgen_path: str, data_path: str, scale_factor: int):
        '''
        TPC-DS data generator. Interface to the `dsdgen` tool provided by
        the Transaction Processing Performance Council.

        `dsdgen` should be downloaded in the directory given by `dbgen_path` and
        a Makefile created, but this program will recompile and execute dsdgen
        with the given `scale_factor`.

        After the data is created, it can be loaded into the specified database
        replicas.

        Used QRLIT as a reference: https://github.com/DBarbosaDev/QRLIT/blob/main/db_env/tpch/TpchGenerator.py

        :param replicas: the databases
        :param dbgen_path: the location of the directory where dbgen will be compiled (fully qualified)
        :param data_path: the desired data directory for the output table data/queries/refresh functions
        :param scale_factor: the TPC-DS scale factor (usually 1 for our experiments)
        '''
        self.replicas = replicas
        self.dbgen_path = dbgen_path
        self.data_path = data_path
        self.scale_factor = str(scale_factor)
        self.dbname = replicas[0].dbname
        self.root_dir = os.path.dirname(os.path.realpath(__file__))
    
    def generate(self, rng_seed: int | None = None):
        '''
        Generates the table and query data for the
        TPC-H benchmark, according to the parameters that were
        passed to the constructor of the Generator class.

        Creates the specified data directory (if it does not already exist),
        compiles dbgen (or recompiles it), and generates the data.
        '''
        if rng_seed == None:
            rng_seed = random.randrange(1_000_000_000, 9_999_999_999)
        rng_seed = str(rng_seed)

        self._create_directories()
        self._compile_dsdgen()
        self._create_table_data(rng_seed)
        self._create_queries(rng_seed)

    def load_database(self):
        '''
        Loads the data into the database.

        (1) Resets each database, creates the schemas, and loads the generated table data.

        (2) Creates the default primary key/foreign key constraints on those tables.
        '''
        connections = [Connection(replica) for replica in self.replicas]
        tables = []

        for table_file in glob.glob(f'{self.data_path}/tables/*.dat'):
            name = os.path.basename(table_file)
            tables.append(name.split('.')[0])

        self._reset_database(connections, tables)
        self._create_schemas(connections)
        self._load_table_data(connections)
        self._create_keys(connections)

        for c in connections:
            c.close()

    def read_data(self) -> list[str]:
        '''
        Loads the queries into memory.

        :returns queries: the 99 generated queries to be executed according to the stream order given in the specification
        '''
        return self._load_queries()
    
    def _create_directories(self):
        logging.debug(f'creating data directories under {self.data_path}')
        os.makedirs(f'{self.data_path}/tables', exist_ok=True)
        os.makedirs(f'{self.data_path}/queries', exist_ok=True)
        os.makedirs(f'{self.data_path}/schema', exist_ok=True)

    def _compile_dsdgen(self):
        logging.debug(f'attempting to compile TPC-DS dsdgen at {self.dbgen_path}')
        subprocess.run(['make', 'CC="gcc-9"'], cwd=self.dbgen_path)
    
    def _create_table_data(self, rng_seed: str):
        logging.debug(f'creating table data for scale factor {self.scale_factor}')
        subprocess.run([f'{self.dbgen_path}/dsdgen',
                        '-DIR', f'{self.data_path}/tables',
                        '-SCALE', self.scale_factor,
                        '-TERMINATE', 'N',
                        '-RNGSEED', rng_seed
                        ], cwd=self.dbgen_path)

        shutil.copy(f'{self.dbgen_path}/tpcds.sql', f'{self.data_path}/schema/dss.ddl')
        shutil.copy(f'{self.dbgen_path}/tpcds_ri.sql', f'{self.data_path}/schema/schema_keys.sql')

    def _create_queries(self, rng_seed: str):
        logging.debug(f'creating TPC-DS query data')

        for i in range(1, 99 + 1):
            with open(f'{self.data_path}/queries/{i}.sql', 'w') as outfile:
                subprocess.run([f'{self.dbgen_path}/dsqgen',
                                '-SCALE', self.scale_factor,
                                '-RNGSEED', rng_seed,
                                '-TEMPLATE', f'query{i}.tpl',
                                '-DIALECT', 'netezza',
                                '-DIRECTORY', os.path.normpath(os.path.join(self.dbgen_path, '..', 'query_templates')),
                                '-FILTER', 'Y'],
                               cwd=self.dbgen_path,
                               stdout=outfile)
    
    def _reset_database(self, connections: list[Connection], tables: list[str]):
        '''
        Drops the tables specified.

        :param tables: a list of table names
        '''
        logging.debug(f'dropping existing tables: {tables}')
        for c in connections:
            with c.conn().cursor() as cur:
                for table in tables:
                    cur.execute(f'DROP TABLE IF EXISTS {table} CASCADE')

    def _create_schemas(self, connections: list[Connection]):
        logging.info('creating the schemas for tables')
        for c in connections:
            with c.conn().cursor() as cur:
                with open(f'{self.data_path}/schema/dss.ddl', 'r') as infile:
                    cur.execute(infile.read())

    def _create_keys(self, connections: list[Connection]):
        logging.info('creating primary and foreign keys')
        for c in connections:
            with c.conn().cursor() as cur:
                with open(f'{self.data_path}/schema/schema_keys.sql', 'r') as infile:
                    cur.execute(infile.read())
    
    def _load_table_data(self, connections: list[Connection]):
        for table_file in glob.glob(f'{self.data_path}/tables/*.dat'):
            table = os.path.basename(table_file).split('.')[0]
            logging.info(f'loading data into {table}')
            for num, c in enumerate(connections):
                logging.debug(f'loading to replica {num}')
                with c.conn().cursor() as cur:
                    with cur.copy(f'COPY {table} FROM STDIN (format csv, delimiter \'|\')') as copy:
                        with open(table_file, 'r') as input:
                            while data := input.read(TABLE_BLOCK_SIZE):
                                copy.write(data)

    def _load_queries(self) -> list[str]:
        logging.info('reading queries')
        queries = []

        for i in range(1, 23):
            with open(f'{self.data_path}/queries/{i}.sql', 'r') as infile:
                queries.append(infile.read())
        
        return queries
