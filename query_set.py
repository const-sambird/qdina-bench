import logging
import time
from multiprocessing import Queue
from connection import Connection
from replica import Replica

class QuerySet:
    def __init__(self, num: int, queries: list[str], templates: list[int], replica: Replica, timer_queue: Queue):
        '''
        Runs a set of queries on a single replica. Puts the amount of time
        each query execution takes in the provided timer queue.

        :param num: which replica is this?
        :param queries: the list of queries to be executed on this node
        :param templates: which query is which, for logging purposes
        :param replica: the replica object this query set is to be executed on
        :param timer_queue: the queue to pass execution times back to the main process
        '''
        self.num = num
        self.queries = queries
        self.n_queries = len(queries)
        self.templates = templates
        self.connection = Connection(replica)
        self.cursor = self.connection.conn().cursor()
        self.timer_queue = timer_queue
    
    def run(self):
        '''
        Run the queries and measure how long they take to execute.
        '''
        times = []
        total_tic = time.time()

        for i, query in enumerate(self.queries):
            logging.debug(f'R{self.num}: execute {i + 1}/{self.n_queries}: Q{self.templates[i] + 1}')

            tic = time.time()
            self.cursor.execute(query)
            toc = time.time()

            logging.debug(f'R{self.num}:Q{self.templates[i] + 1}: {round(toc - tic, 2)}s')

            times.append(toc - tic)
        
        total_toc = time.time()

        self.timer_queue.put({
            'total': total_toc - total_tic,
            'times': times
        })

        logging.info(f'replica {self.num} completed in {round(total_toc - total_tic, 2)}s')

        self.cursor.close()
        self.connection.close()
