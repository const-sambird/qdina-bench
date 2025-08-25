import psycopg
import glob
import os

CONNECTION_STRING = 'host=127.0.0.1 port=5432 dbname=tpcdsdb user=sam password=K7wJReQcW86SUYyhXaLPvsE2jnVBGMxNHzDfbCkt5u39FTAdrq'
TABLE_BLOCK_SIZE = 5_120_000
ROOT_DIR = '/tmp/ds'

def upload_tables():
    tables = glob.glob(f'{ROOT_DIR}/*.dat')
    for table_file in tables:
        table = os.path.basename(table_file).split('.')[0]
        with psycopg.connect(CONNECTION_STRING) as conn:
            with conn.cursor() as cur:
                with cur.copy(f'COPY {table} FROM STDIN (format csv, delimiter \',\')') as copy:
                    with open(table_file, 'r') as input:
                        while data := input.read(TABLE_BLOCK_SIZE):
                            copy.write(data)

if __name__ == '__main__':
    upload_tables()
