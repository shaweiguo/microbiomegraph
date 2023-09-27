from multiprocessing import Pool
import requests
import biom
from loguru import logger
import tempfile
import os
from queue import Queue, Empty
from threading import Thread


POOL_SIZE = 4
THREAD_POOL_SIZE = 4


def get_file_name(url):
    idx = url.rfind('/')
    file_name = url[idx + 1:]
    return file_name


def download(url):
    try:
        r = requests.get(url)
        tmp_dir = tempfile.gettempdir()
        file_name = get_file_name(url)
        biom_path = os.path.join(tmp_dir, file_name)
        with open(biom_path, "wb") as f:
            f.write(r.content)
        logger.info('{url} download complete.')
        return biom_path        
    except Exception as e:
        logger.error(f"\nException in download_url() for {url}:", e)
        return None


def save(biom_path, tsv_path):
    try:
        biom_table = biom.load_table(biom_path)
        with open(tsv_path, "w") as f:
            biom_table.to_tsv(
                header_key="taxonomy", header_value="taxonomy", direct_io=f
            )
        logger.info(f'{tsv_path} save complete.')
        return tsv_path        
    except Exception as e:
        logger.error(f"\nException in save biom file for {biom_path}:", e)
        return None


def scrape(urls, tsv_dir, work_queue, results_queue):
    while not work_queue.empty():
        try:
            item = work_queue.get(block=False)
        except Empty:
            break
        else:
            results_queue.put()


def download(urls, tsv_dir):
    with Pool(POOL_SIZE) as p:

        p.map(download, urls)
        p.map(save, urls, tsv_dir)


def main():
    urls = [
        'https://downloads.hmpdacc.org/ihmp/ibd/genome/microbiome/wgs/analysis/hmmrc/MSM5LLDE_ecs_relab.biom',
        'https://downloads.hmpdacc.org/ihmp/ibd/genome/microbiome/wgs/analysis/hmscp/MSM5LLDE_taxonomic_profile.biom',
        'https://downloads.hmpdacc.org/ihmp/ibd/genome/microbiome/wgs/analysis/hmmrc/MSM5LLDE_pathabundance_relab.biom',
        'https://downloads.hmpdacc.org/ihmp/ibd/genome/microbiome/wgs/analysis/hmmrc/MSM5LLDE_genefamilies_relab.biom',
        'https://downloads.hmpdacc.org/ihmp/ibd/genome/microbiome/wgs/analysis/hmscp/MSM5LLDM_taxonomic_profile.biom'
    ]
    tsv_dir = '../../data/interim/tsv'

    work_queue = Queue()
    for url in urls:
        item = {
            'src': url,
            'dst': os.path.join(tsv_dir, get_file_name(url))
        }
        work_queue.put(item)
    
    threads = [
        Thread(target=)
    ]
    