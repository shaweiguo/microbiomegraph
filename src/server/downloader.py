import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq.results import Results
from dramatiq.results.backends import RedisBackend
import requests
import biom
from loguru import logger
import tempfile
import os


broker = RedisBroker(host="localhost", port=6379)
result_backend = RedisBackend(host="localhost", port=6379)
broker.add_middleware(Results(backend=result_backend))
dramatiq.set_broker(broker)


def get_file_name(url):
    idx = url.rfind('/')
    file_name = url[idx + 1:]
    return file_name


@dramatiq.actor(max_retries=3)
def download(url, tsv_path):
    try:
        r = requests.get(url)
        tmp_dir = tempfile.gettempdir()
        file_name = get_file_name(url)
        biom_path = os.path.join(tmp_dir, file_name)
        with open(biom_path, "wb") as f:
            f.write(r.content)

        biom_table = biom.load_table(biom_path)

        with open(tsv_path, "w") as f:
            biom_table.to_tsv(
                header_key="taxonomy", header_value="taxonomy", direct_io=f
            )
        
        logger.info('{url} download complete.')
        
    except Exception as e:
        logger.error(f"\nException in download_url() for {url}:", e)
