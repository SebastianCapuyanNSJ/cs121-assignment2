from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:

            # get next url & try to download
            tbdUrl = self.frontier.get_tbd_url()
            if not tbdUrl:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            
            try:
                resp = download(tbdUrl, self.config, self.logger)
            except Exception as e:
                self.logger.error(f"Error downloading {tbdUrl}: {e}")
                self.frontier.mark_url_complete(tbdUrl)
                time.sleep(self.config.time_delay) # Wait a bit before retrying
                continue

            self.logger.info(
                f"Downloaded {tbdUrl}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            try:
                # extract links from page and add to frontier
                scrapedUrls = scraper.scraper(tbdUrl, resp)
                for scrapedUrl in scrapedUrls:
                    self.frontier.add_url(scrapedUrl)
            except Exception as e:
                self.logger.error(f"Error scraping {tbdUrl}: {e}")
            # mark url as finished so frontier can continue
            finally:
                self.frontier.mark_url_complete(tbdUrl)
                time.sleep(self.config.time_delay)
