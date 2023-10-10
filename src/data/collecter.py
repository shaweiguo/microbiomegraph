from loguru import logger
import threading
import queue
import concurrent.futures
import os
import re
import pandas as pd
import requests
from biom import load_table as load_biom


def get_file_name(url):
    idx = url.rfind('/')
    file_name = url[idx + 1:]
    return file_name


def get_urls(master_list_file_path, url_queue):
    df = pd.ExcelFile(master_list_file_path).parse('List')
    for url in df['urls'].values:
        url_queue.put(url)


def done(future):
    future.queue.put(future.result())
    # logger.info(f'{future.result()} done.')


def download(url):
    req = requests.get(url)
    logger.info(f'{url} downloaded.')
    return {'url': url, 'content': req.content}
    # download_queue.put({'url': url, 'content': req.content})


def download_manager(num, url_queue, download_queue):
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=num, thread_name_prefix="download"
    ) as executor:
        while True > 0:
            url = url_queue.get()
            future = executor.submit(download, url)
            future.queue = download_queue
            future.add_done_callback(done)


def biom_save(biom, dir):
    path = os.path.join(dir, get_file_name(biom['url']))
    with open(path, "wb") as f:
        f.write(biom['content'])
    logger.info(f'{path} saved.')
    return {'path': path}
    # biom_queue.put({'path': path})


def biom_save_mamager(num, biom_dir, download_queue, biom_queue):
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=num, thread_name_prefix="biom_save"
    ) as executor:
        while True:
            biom = download_queue.get()
            future = executor.submit(biom_save, biom, biom_dir)
            future.queue = biom_queue
            future.add_done_callback(done)


def tsv_transform(biom_path, tsv_dir):
    biom_table = load_biom(biom_path)
    filename = get_file_name(biom_path)
    filename = filename.replace('.biom', '.tsv')
    tsv_path = os.path.join(tsv_dir, filename)
    with open(tsv_path, 'w') as f:
        biom_table.to_tsv(
            header_key='taxonomy',
            header_value='taxonomy',
            direct_io=f)
    logger.info(f'{tsv_path} saved.')
    return {'path': tsv_path}


def tsv_transform_manager(num, tsv_dir, in_queue, out_queue):
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=num, thread_name_prefix="tsv"
    ) as executor:
        while True > 0:
            item = in_queue.get()
            future = executor.submit(tsv_transform, item['path'], tsv_dir)
            future.queue = out_queue
            future.add_done_callback(done)


def csv_transform(tsv_path, csv_dir):
    with open(tsv_path, "r") as fin:
        lines = fin.readlines()
        lines[1] = lines[1][1:].replace("OTU ID", "OTU_ID")
        lines[1] = re.sub("\t.*_Abundance", "\tAbundance", lines[1])
        lines[1] = lines[1].replace("-RPKs", "_RPKs")
        new_lines = [re.sub(".s__", "|s__", s) for s in lines]

    filename = get_file_name(tsv_path)
    filename = filename.replace('.tsv', '.csv')
    csv_path = os.path.join(csv_dir, filename)

    with open(csv_path, "w") as fout:
        fout.writelines(new_lines[1:])  # removes the first line before saving
    logger.info(f'{csv_path} saved.')

    return {'path': csv_path}


def csv_transform_manager(num, csv_dir, in_queue, out_queue):
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=num, thread_name_prefix="csv"
    ) as executor:
        while True > 0:
            item = in_queue.get()
            future = executor.submit(csv_transform, item['path'], csv_dir)
            future.queue = out_queue
            future.add_done_callback(done)


def main():
    MASTER_LIST_FILE_PATH = '/home/sha/work/microbiome/data/meta/Master_List.xlsx'
    BIOM_DIR = './tmp/biom'
    TSV_DIR = './tmp/tsv'
    CSV_DIR = './tmp/csv'
    DOWNLOAD_THREADS = 5
    SAVE_THREADS = 3
    URL_QUEUE = queue.Queue()
    DOWNLOAD_QUEUE = queue.Queue()
    BIOM_QUEUE = queue.Queue()
    TSV_QUEUE = queue.Queue()
    CSV_QUEUE = queue.Queue()
    get_urls(MASTER_LIST_FILE_PATH, URL_QUEUE)

    downloader = threading.Thread(
        target=download_manager,
        args=(DOWNLOAD_THREADS, URL_QUEUE, DOWNLOAD_QUEUE))
    downloader.start()

    biom_saver = threading.Thread(
        target=biom_save_mamager,
        args=(SAVE_THREADS, BIOM_DIR, DOWNLOAD_QUEUE, BIOM_QUEUE))
    biom_saver.start()

    tsv_transformer = threading.Thread(
        target=tsv_transform_manager,
        args=(SAVE_THREADS, TSV_DIR, BIOM_QUEUE, TSV_QUEUE))
    tsv_transformer.start()

    csv_transformer = threading.Thread(
        target=csv_transform_manager,
        args=(SAVE_THREADS, CSV_DIR, TSV_QUEUE, CSV_QUEUE))
    csv_transformer.start()

    downloader.join()
    biom_saver.join()
    tsv_transformer.join()
    csv_transformer.join()
    logger.info("all done.")


if __name__ == '__main__':
    main()
