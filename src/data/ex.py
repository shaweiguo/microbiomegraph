import multiprocessing as mp
from loguru import logger
import threading, queue
import concurrent.futures
import os

def task(n):
    """
    Perform the task operation - In real scenario, this method can be replaced by any actual task 
    """
    print(f"Task {n} is running on process {os.getpid()}")
    total = sum(range(n))
    return total

def main_process(start, end):
    """
    Function that creates multiple threads to handle tasks in its own process.
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        tasks = [n for n in range(start, end+1)] # creating tasks
        results = list(executor.map(task, tasks))

    print(f'Process {os.getpid()} finished, results: {results}')
    return results

def distribute_tasks(total_tasks, num_processes):
    """
    The main function that creates multiple processes, each of which handles a portion of tasks.
    """
    with concurrent.futures.ProcessPoolExecutor() as executor:
        tasks_each_process = total_tasks // num_processes
        all_tasks = [(i*tasks_each_process+1, (i+1)*tasks_each_process) for i in range(num_processes)]
        executor.map(main_process, *zip(*all_tasks))


def ex_parallel():
    num_processes = 4 
    total_tasks = 100 
    distribute_tasks(total_tasks, num_processes)


def f(s):
    import time, random
    time.sleep(random.random())
    return s + s


def runner(s):
    import os
    logger.info(f"{os.getpid()}: {s}")
    return s, f(s)


def make_dict(strings):
    # mgr = mp.Manager()
    # d = mgr.dict()
    # workers = []
    # for s in strings:
    #     p = mp.Process(target=runner, args=(s, d))
    #     workers.append(p)
    #     p.start()
    # for p in workers:
    #     p.join()
    # return {**d}
    with mp.Pool() as pool:
        d = dict(pool.imap_unordered(runner, strings))
        return d


def ex_pool():
    d = make_dict(["a", "b", "c"])
    logger.info(f"{d}")


class ExternalInterfacing(threading.Thread):
    def __init__(self, external_callable, **kwds):
        super().__init__(**kwds)
        self.daemon = True
        self._external_callable = external_callable
        self._request_queue = queue.Queue()
        self._result_queue = queue.Queue()
        self.start()
    
    def request(self, *args, **kwds):
        self._request_queue.put((args, kwds))
        return self._result_queue.get()
    
    def run(self):
        while True:
            args, kwds = self._request_queue.get()
            self._result_queue.put(self._external_callable(*args, **kwds))


class Serializer(threading.Thread):
    def __init__(self, **kwds):
        super().__init__(**kwds)
        self.daemon = True
        self._request_queue = queue.Queue()
        self._result_queue = queue.Queue()
        self.start()
    
    def apply(self, callable, *args, **kwds):
        self._request_queue.put((callable, args, kwds))
        return self._result_queue.get()
    
    def run(self):
        while True:
            callable, args, kwds = self._request_queue.get()
            self._result_queue.put(callable(*args, **kwds))


class Worker(threading.Thread):
    IDlock = threading.Lock()
    request_ID = 0

    def __init__(self, request_queue, result_queue, **kwds):
        super().__init__(**kwds)
        self.daemon = True
        self._request_queue = request_queue
        self._result_queue = result_queue
        self.start()
    
    def work(self, callable, *args, **kwds):
        with self.IDlock:
            Worker.request_ID += 1
            self._request_queue.put((Worker.request_ID, callable, args, kwds))
            return Worker.request_ID
    
    def run(self):
        while True:
            request_ID, callable, args, kwds = self._request_queue.get()
            self._result_queue.put((request_ID, callable(*args, **kwds)))


def ex_work():
    import time, random, operator
    request_quue = queue.Queue()
    result_queue = queue.Queue()

    number_of_workers = 3
    workers = [Worker(request_quue, result_queue) for _ in range(number_of_workers)]
    work_requests = {}

    operations = {
        '+': operator.add,
        '-': operator.sub,
        '*': operator.mul,
        '/': operator.truediv,
        '%': operator.mod,
    }

    def pick_a_worker():
        return random.choice(workers)
    
    def make_work():
        o1 = random.randrange(2, 10)
        o2 = random.randrange(2, 10)
        op = random.choice(list(operations))
        return f'{o1} {op} {o2}'
    
    def slow_evaluate(expression):
        time.sleep(random.randrange(1, 5))
        op1, oper, op2 = expression.split()
        arith_func = operations[oper]
        return arith_func(int(op1), int(op2))
    
    def show_results():
        while True:
            try:
                completed_id, results = result_queue.get_nowait()
            except queue.Empty:
                return
            work_expression = work_requests.pop(completed_id)
            logger.info(f"Result {completed_id}: {work_expression} -> {results}")
    
    for i in range(10):
        expression = make_work()
        worker = pick_a_worker()
        request_id = worker.work(slow_evaluate, expression)
        work_requests[request_id] = expression
        logger.info(f"Submitted request {request_id}: {expression}")
        time.sleep(0.01)
        show_results()
    
    while work_requests:
        time.sleep(1.0)
        show_results()


def main():
    ex_work()


if __name__ == "__main__":
    main()
