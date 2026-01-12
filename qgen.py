import logging
import subprocess
import os
import glob
import shutil
import random
import argparse

ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
DBGEN_DIR = os.path.join(ROOT_DIR, './tpc-h/dbgen')
DBGEN_DIR = os.path.normpath(DBGEN_DIR)
RNG_SEED = random.randrange(1_000_000_000, 9_999_999_999)

def get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--scale-factor', type=int, default=10, help='tpc-h scale factor for generated queries')
    parser.add_argument('-n', '--queries-per-template', type=int, default=50, help='queries to generate from each template')
    parser.add_argument('-o', '--out-path', type=str, default='/proj/qdina-PG0/qdina-1100', help='location to write generated queries')

    return parser.parse_args()

def _compile_dbgen():
    logging.debug(f'attempting to compile TPC-H dbgen at {DBGEN_DIR}')
    subprocess.run('make', cwd=DBGEN_DIR)

def _move_query_templates():
    existing_templates = glob.glob(f'{DBGEN_DIR}/queries/*.sql')
    corrected_templates = glob.glob(f'{ROOT_DIR}/tpch-templates/*.sql')

    for template in existing_templates:
        os.remove(template)
    
    for template in corrected_templates:
        shutil.copy(template, f'{DBGEN_DIR}/queries')

def _create_queries(outpath, num_per_template, scale_factor):
    logging.debug(f'* creating TPC-H query data')

    for i in range(1, 23):
        logging.debug(f'+ {i} / 22')
        for j in range(num_per_template):
            if j % 10 == 0:
                logging.debug(f'-   {j}')
            with open(f'{outpath}/{i}_{j}.sql', 'w') as outfile:
                subprocess.run([f'{DBGEN_DIR}/qgen', '-s', str(scale_factor), '-r', str(RNG_SEED + j), str(i)],
                                cwd=DBGEN_DIR,
                                env=dict(os.environ, DSS_QUERY=f'{DBGEN_DIR}/queries'),
                                stdout=outfile)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    args = get_arguments()

    _compile_dbgen()
    _move_query_templates()
    _create_queries(args.out_path, args.queries_per_template, args.scale_factor)
