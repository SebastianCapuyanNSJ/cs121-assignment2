import os
import shelve
import time

from threading import Thread, RLock
from queue import Queue, Empty
from urllib.parse import urlparse

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        
        # lock to protect domainQueues, domainLastAccess, inProcessCount, and saved_hashes
        self.lock = RLock()
        self.domainQueues = {}   
        self.domainLastAccess = {} 
        self.inProcessCount = 0 
        self.saved_hashes = set()

        if os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        
        # shelve db for resuming crawl if interrupted
        with shelve.open(self.config.save_file) as save:
            if not save:
                self.logger.info(f"Did not find save file {self.config.save_file} (or it was empty), starting from seed.")
                for url in self.config.seed_urls:
                    self._add_url_to_save(save, url)
            else:
                self.logger.info(f"Loading state from {self.config.save_file}...")
                self._parse_save_file(save)

    # load old saved urls, add unfinished urls to queue
    def _parse_save_file(self, save):
        total_count = len(save)
        for urlhash, (url, completed) in save.items():
            self.saved_hashes.add(urlhash)
            if not completed and is_valid(url):
                self.addToDomainQueue(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    # only save and queue url if we haven't crawled already
    def _add_url_to_save(self, save, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.saved_hashes:
            save[urlhash] = (url, False)
            self.saved_hashes.add(urlhash)
            self.addToDomainQueue(url)

    # group urls by domain for politeness and add to queue
    def addToDomainQueue(self, url):
        domain = urlparse(url).netloc
        if domain not in self.domainQueues:
            self.domainQueues[domain] = []
        self.domainQueues[domain].append(url)

    # pick the next url to crawl with politeness config
    def get_tbd_url(self):
        while True:
            with self.lock:
                totalQueued = sum(len(q) for q in self.domainQueues.values())
                if totalQueued == 0 and self.inProcessCount == 0:
                    return None

                currentTime = time.time()
                readyUrl = None
                readyDomain = None

                for domain, queue in self.domainQueues.items():
                    if not queue:
                        continue
                    
                    lastAccess = self.domainLastAccess.get(domain, 0)
                    if currentTime - lastAccess >= self.config.time_delay:
                        readyUrl = queue.pop(0)
                        readyDomain = domain
                        break
                
                if readyUrl:
                    self.domainLastAccess[readyDomain] = time.time()
                    self.inProcessCount += 1
                    return readyUrl
            time.sleep(0.1)

    def add_url(self, url):
        with self.lock:
            url = normalize(url)
            urlhash = get_urlhash(url)
            if urlhash in self.saved_hashes:
                return
            with shelve.open(self.config.save_file) as save:
                self._add_url_to_save(save, url)
    
    # manage inProcessCount after worker finishes crawling a url
    def mark_url_complete(self, url):
        with self.lock:
            urlhash = get_urlhash(url)
            with shelve.open(self.config.save_file) as save:
                if urlhash not in save:
                    self.logger.error(
                        f"Completed url {url}, but have not seen it before.")
                else:
                    save[urlhash] = (url, True)
            
            self.inProcessCount -= 1
