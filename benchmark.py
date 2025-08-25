import logging
import random
import time

from connection import Connection

class Benchmark:
    def __init__(self, queries, templates, replicas, routes, config):
        '''
        Benchmarks the performance of created indexes by finding the execution
        time of every query in the workload. Reports the total execution time
        as well as the execution time for each query.
        '''
        self.queries = queries
        self.templates = templates
        self.n_queries = len(queries)
        self.n_templates = len(list(set(templates)))
        self.connections = [Connection(r) for r in replicas]
        self.cursors = [c.conn().cursor() for c in self.connections]
        self.routes = routes
        self.config = config
        self.times = [0 for _ in range(self.n_templates)]
        self.order = [i for i in range(self.n_queries)]

        self._create_indexes()
    
    def _create_indexes(self):
        '''
        Create the indexes in the index configuration that the recommendation
        program (qDINA) created.
        '''
        logging.info('creating indexes!')

        indexes_created = 0

        for i_rep, config in enumerate(self.config):
            cur = self.cursors[i_rep]
            for index in config:
                indexes_created += 1
                cur.execute(f'CREATE INDEX idx_{indexes_created} ON {index[0]} ({','.join(index[1])})')
    
    def run(self) -> tuple[float, list[float]]:
        '''
        Run the benchmark.

        :returns total: the sum of the query execution times
        :returns times: how long each query took to execute, in workload (not shuffled) order
        '''
        random.shuffle(self.order)

        for i, query_num in enumerate(self.order):
            query = self.queries[query_num]
            template = self.templates[query_num]
            logging.debug(f'execute {i + 1}/{self.n_queries}: Q{template + 1}')
            replica = self.routes[template]

            tic = time.time()
            self.cursors[replica].execute(query)
            toc = time.time()

            self.times[template] += toc - tic
            logging.debug(f'Q{template + 1} completed in {round(toc - tic, 2)}s')
    
        total = sum(self.times)
        logging.debug(f'all queries completed in {round(total, 2)}s')

        return total, self.times
