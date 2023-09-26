import os
import sys
import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq.results import Results
from dramatiq.results.backends import RedisBackend

broker = RedisBroker(host="localhost")
result_backend = RedisBackend(host="localhost")
broker.add_middleware(Results(backend=result_backend))
dramatiq.set_broker(broker)

path = os.path.abspath('../server')
sys.path.append(path)

import downloader


urls = [
    'https://downloads.hmpdacc.org/ihmp/ibd/genome/microbiome/wgs/analysis/hmmrc/MSM5LLDE_ecs_relab.biom',
    'https://downloads.hmpdacc.org/ihmp/ibd/genome/microbiome/wgs/analysis/hmscp/MSM5LLDE_taxonomic_profile.biom',
    'https://downloads.hmpdacc.org/ihmp/ibd/genome/microbiome/wgs/analysis/hmmrc/MSM5LLDE_pathabundance_relab.biom',
    'https://downloads.hmpdacc.org/ihmp/ibd/genome/microbiome/wgs/analysis/hmmrc/MSM5LLDE_genefamilies_relab.biom',
    'https://downloads.hmpdacc.org/ihmp/ibd/genome/microbiome/wgs/analysis/hmscp/MSM5LLDM_taxonomic_profile.biom'
]


def get_file_name(url):
    idx = url.rfind('/')
    file_name = url[idx + 1:]
    return file_name


def download_bioms(urls, tsv_dir):
    for url in urls:
        biom_file_name = get_file_name(url)
        idx = biom_file_name.index('.')
        name = biom_file_name[:idx]
        tsv_path = os.path.join(tsv_dir, name + '.tsv')
        downloader.download(url, tsv_path)


def main():
    download_bioms(urls, '../../data/interim/tsv')


if __name__ == '__main__':
    main()
