import logging
import random
import time

from multiprocessing import Process, Queue
from connection import Connection
from query_set import QuerySet

class Benchmark:
    def __init__(self, queries, templates, replicas, routes, config, create_indexes):
        '''
        Benchmarks the performance of created indexes by finding the execution
        time of every query in the workload. Reports the total execution time
        as well as the execution time for each query.
        '''
        self.queries = queries
        self.templates = templates
        self.n_queries = len(queries)
        self.n_templates = len(list(set(templates)))
        self.replicas = replicas
        self.connections = [Connection(r) for r in replicas]
        self.cursors = [c.conn().cursor() for c in self.connections]
        self.routes = routes
        self.config = config
        self.times = [0 for _ in range(self.n_templates)]
        self.order = [i for i in range(self.n_queries)]

        if create_indexes:
            self._create_indexes()
        else:
            logging.warning('skipping index creation!')
    
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

        :returns total: the overall query execution time
        :returns times: how long each query took to execute, in workload (not shuffled) order
        '''
        random.shuffle(self.order)

        replica_workloads = [[] for _ in self.replicas]
        replica_templates = [[] for _ in self.replicas]

        for query_num in self.order:
            query = self.queries[query_num]
            template = self.templates[query_num]
            replica = self.routes[template]

            replica_workloads[replica].append(query)
            replica_templates[replica].append(template)
        
        query_sets = []
        timer_queues = [Queue() for _ in self.replicas]

        for i, replica in enumerate(self.replicas):
            query_sets.append(QuerySet(i, replica_workloads[i], replica_templates[i], replica, timer_queues[i]))
        
        processes = [Process(target=qs.run) for qs in query_sets]

        tic = time.time()
        [p.start() for p in processes]
        [p.join() for p in processes]
        toc = time.time()

        for queue in timer_queues:
            info = queue.get()
            for i, time in enumerate(info['times']):
                template = replica_templates[i]
                self.times[template] += time
    
        total = toc - tic
        logging.debug(f'all queries completed in {round(total, 2)}s')

        return total, self.times
    
    def destroy_indexes(self):
        indexes_destroyed = 0

        for i_rep, config in enumerate(self.config):
            cur = self.cursors[i_rep]
            for index in config:
                indexes_destroyed += 1
                cur.execute(f'DROP INDEX idx_{indexes_destroyed}')
        
        logging.debug('dropped indexes')
