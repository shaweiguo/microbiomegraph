from loguru import logger
import threading
import queue
import concurrent.futures
import os
import time
import random
import pandas as pd


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
            self._request_queue.put((
                Worker.request_ID, callable, args, kwds
            ))
            return Worker.request_ID
    
    def run(self):
        while True:
            request_ID, callable, args, kwds = self._request_queue.get()
            self._result_queue.put((request_ID, callable(*args, **kwds)))


class ThreadWorker(threading.Thread):
    def __init__(self, request_queue, result_queue, **kwds):
        super().__init__(**kwds)
        self.daemon = True
        self._request_queue = request_queue
        self._result_queue = result_queue
        self.start()
    
    def run(self):
        while True:
            callable, args, kwds = self._request_queue.get()
            self._result_queue.put((callable(*args, **kwds)))


def get_urls(master_list_file_path, url_queue):
    df = pd.ExcelFile(master_list_file_path).parse('List')
    for url in df['urls'].values:
        url_queue.put(url)


def download(num, in_queue, out_queue):
    request_queue = queue.Queue()
    result_queue = queue.Queue()

    downloaders = [
        Worker(request_queue, result_queue)
        for _ in range(num)
    ]
    requests = {}

    def pick_a_downloader():
        return random.choice(downloaders)
    
    def get_file_name(url):
        idx = url.rfind('/')
        file_name = url[idx + 1:]
        return file_name
    
    def get_biom(url):
        import requests
        req = requests.get(url)
        out_queue.put({'url': url, 'biom': req.content})
        # with open(os.path.join(biom_dir, get_file_name(url)), "wb") as f:
        #     f.write(req.content)
        return {"status": "ok", "url": url}
    
    def show_results():
        while True:
            try:
                completed_id, results = result_queue.get_nowait()
            except queue.Empty:
                return
            url = requests.pop(completed_id)
            logger.info(f"Result {completed_id}: {url} -> {results}")
    
    while in_queue.qsize() > 0:
        url = in_queue.get()
        worker = pick_a_downloader()
        request_id = worker.work(get_biom, url)
        requests[request_id] = request_id
        logger.info(f"Submitted request {request_id}: {request_id}")
        show_results()
    
    while requests:
        show_results()


def save_biom(num, in_queue, out_queue, dir):
    request_queue = queue.Queue()
    result_queue = queue.Queue()

    savers = [
        Worker(request_queue, result_queue)
        for _ in range(num)
    ]
    requests = {}

    def pick():
        return random.choice(savers)
    
    def get_file_name(url):
        idx = url.rfind('/')
        file_name = url[idx + 1:]
        return file_name
    
    def save(biom, path):
        with open(path, "wb") as f:
            f.write(biom)
        out_queue.put({'path': path})
        
        return {"status": "ok", "path": path}
    
    def show_results():
        while True:
            try:
                completed_id, results = result_queue.get_nowait()
            except queue.Empty:
                return
            path = requests.pop(completed_id)
            logger.info(f"Result {completed_id}: {path} -> {results}")
    
    while in_queue.qsize() > 0:
        biom = in_queue.get()
        worker = pick()
        url = biom['url']
        content = biom['biom']
        path = os.path.join(dir, get_file_name(url))
        request_id = worker.work(save, path, content)
        requests[request_id] = request_id
        logger.info(f"Submitted request {request_id}: {request_id}")
        show_results()
    
    while requests:
        show_results()


def biom_download(in_queue, out_queue):
    MASTER_LIST_FILE_PATH = '/home/sha/work/microbiome/data/meta/Master_List.xlsx'
    BIOM_DIR = './tmp/biom'
    DOWNLOAD_THREADS = 5
    SAVE_THREADS = 3
    URLS_QUEUE = queue.Queue()
    BIOM_QUEUE = queue.Queue()
    TSV_QUEUE = queue.Queue()
    # CSV_QUEUE = queue.Queue()
    get_urls(MASTER_LIST_FILE_PATH, URLS_QUEUE)
    # logger.info(f"{urls}")
    download(DOWNLOAD_THREADS, URLS_QUEUE, BIOM_QUEUE)
    save_biom(SAVE_THREADS, BIOM_QUEUE, TSV_QUEUE, BIOM_DIR)
    request_queue = queue.Queue()
    result_queue = queue.Queue()

    downloaders = [
        Worker(request_queue, result_queue)
        for _ in range(num)
    ]
    requests = {}

    def pick_a_downloader():
        return random.choice(downloaders)
    
    def get_file_name(url):
        idx = url.rfind('/')
        file_name = url[idx + 1:]
        return file_name
    
    def get_biom(url):
        import requests
        req = requests.get(url)
        out_queue.put({'url': url, 'biom': req.content})
        # with open(os.path.join(biom_dir, get_file_name(url)), "wb") as f:
        #     f.write(req.content)
        return {"status": "ok", "url": url}
    
    def show_results():
        while True:
            try:
                completed_id, results = result_queue.get_nowait()
            except queue.Empty:
                return
            url = requests.pop(completed_id)
            logger.info(f"Result {completed_id}: {url} -> {results}")
    
    while in_queue.qsize() > 0:
        url = in_queue.get()
        worker = pick_a_downloader()
        request_id = worker.work(get_biom, url)
        requests[request_id] = request_id
        logger.info(f"Submitted request {request_id}: {request_id}")
        show_results()
    
    while requests:
        show_results()


def main():
    MASTER_LIST_FILE_PATH = '/home/sha/work/microbiome/data/meta/Master_List.xlsx'
    BIOM_DIR = './tmp/biom'
    DOWNLOAD_THREADS = 5
    SAVE_THREADS = 3
    URLS_QUEUE = queue.Queue()
    BIOM_QUEUE = queue.Queue()
    TSV_QUEUE = queue.Queue()
    CSV_QUEUE = queue.Queue()
    get_urls(MASTER_LIST_FILE_PATH, URLS_QUEUE)
    # logger.info(f"{urls}")
    download(DOWNLOAD_THREADS, URLS_QUEUE, BIOM_QUEUE)
    save_biom(SAVE_THREADS, BIOM_QUEUE, TSV_QUEUE, BIOM_DIR)


if __name__ == "__main__":
    main()
