#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Simple WebCrawler to restore pages which contain the keyword.
"""

__version__ = '1.0'

# import standard library modules
import time
import optparse
import urllib2
import urlparse
import threading
import sqlite3
import logging
import re
import hashlib
import posixpath
import Queue

# other useful library modules
from bs4 import BeautifulSoup
from bitarray import bitarray

# common file extensions that are not followed if they occur in links
IGNORED_EXTENSIONS = [
    # images
    'mng', 'pct', 'bmp', 'gif', 'jpg', 'jpeg', 'png', 'pst', 'psp', 'tif',
    'tiff', 'ai', 'drw', 'dxf', 'eps', 'ps', 'svg',

    # audio
    'mp3', 'wma', 'ogg', 'wav', 'ra', 'aac', 'mid', 'au', 'aiff',

    # video
    '3gp', 'asf', 'asx', 'avi', 'mov', 'mp4', 'mpg', 'qt', 'rm', 'swf', 'wmv',
    'm4a',

    # other
    'css', 'pdf', 'doc', 'exe', 'bin', 'rss', 'zip', 'rar', 'gz', 'gz2',
]

# upmost logging level.
UPMOST_LOG_LEVEL = logging.CRITICAL

class Counter():
    """Thread-safe integer counter. """
    def __init__(self, success_count=0, fail_count=0):
        self._success_count = success_count;
        self._fail_count = fail_count
        self._match_count = 0
        self._lock = threading.Lock()

    def success(self):
        with self._lock:
            self._success_count += 1

    def fail(self):
        with self._lock:
            self._fail_count += 1

    def bingo(self):
        with self._lock:
            self._match_count += 1

    def success_count(self):
        return self._success_count

    def fail_count(self):
        return self._fail_count

    def bingo_count(self):
        return self._match_count

class Worker(threading.Thread):
    """ Thread crawling web pages from a given url queue"""


    def __init__(self, task_queue, depth, hash_table, hash_table_lock,
            db_connect, process_counter, keyword_re, poll_time_out):
        """Construcion."""
        threading.Thread.__init__(self)
        self.daemon = True
        self._dismissed = threading.Event()
        self.task_queue = task_queue
        self.depth = depth
        self._hash_table = hash_table
        self._hash_table_lock = hash_table_lock
        self.db_connect = db_connect
        self.process_counter = process_counter
        self.keyword_re = keyword_re
        self.poll_time_out = poll_time_out
        self.db_cursor = self.db_connect.cursor()
        self.start()

    def run(self):
        """Get task from task_queue, then crawling the webpage.

        In the crawling procedure, new task will put into taskqueue.
        It seem like Breadth-First-Crawl stuff.
        """

        while True:
            if self._dismissed.isSet():
                #we are dissmissed, commit DB transaction, then break out.
                #self.db_connect.commit()
                break

            try:
                #get next task from task_queue, open it and try to
                #parse it with BeautifulSoup.
                url, depthlevel = self.task_queue.get(True, self.poll_time_out)
                logging.debug("Get a new URL from task_queue: %d: %s" % (depthlevel, url))

                page = urllib2.urlopen(url)
                #use the charset defined in the meta tag as from_encoding
                #parameter for BeautifulSoup, None if there is no charset
                #defined, BeautifulSoup will use encoding it finds in the
                #webpage document.
                page_charset = page.headers.getparam('charset')
                soup = BeautifulSoup(page, from_encoding=page_charset)
                page_encoding = page_charset or soup.original_encoding
                logging.debug("Parsed URL successfully: %s with encoding %s"
                        % (url, page_encoding))

                #search the specified keyword throughout the page text content.
                if soup.find(text=self.keyword_re):
                    self.process_counter.bingo()
                    logging.debug("Find a page which contains the keyword, try to insert it to DB...")
                    self.db_cursor.execute(Spider.INSERT_RECORD_TO_MATCH_TABLE,
                            (url, depthlevel, soup.encode(page_encoding)))

                #get all links in this page, parse all over them one by one,
                #filter useless(or invalid) links, we just put urls which we care
                #about into the task_queue
                if depthlevel < self.depth:
                    logging.debug("length of soup(a): %d" % len(soup('a')))
                    for link in soup.find_all('a'):
                        if ('href' in dict(link.attrs)):
                            parsed_link = urlparse.urljoin(url, link['href'])
                            # remove location portion
                            parsed_link = parsed_link.split('#')[0]

                            logging.debug("------parsed_link: %s" % parsed_link)
                            if parsed_link[:4] != 'http' or \
                                    self.url_has_any_extension(parsed_link, IGNORED_EXTENSIONS):
                                continue
                            logging.debug("parsed_link: %s" % parsed_link)

                            md5hashtool = hashlib.md5()
                            md5hashtool.update(parsed_link)
                            hash_value = int(md5hashtool.hexdigest(), 16)
                            hash_index = hash_value&(Spider.HASH_TABLE_SIZE-1)
                            with self._hash_table_lock:
                                if self._hash_table[hash_index]:
                                    logging.warn("duplicatory URL(or hash value confict): %s"
                                            % parsed_link)
                                else:
                                    self._hash_table[hash_index] = True
                                    logging.debug("insert a new task into task_queue, url: %s"
                                            % parsed_link)
                                    self.task_queue.put((parsed_link, depthlevel+1))

                self.task_queue.task_done()
                self.process_counter.success()
            except Queue.Empty:
                #If we don't get a new request from the queue after
                #self._poll_timeout seconds, we jump to the start of
                #the while loop, to give the thread a chance to exit.
                logging.warn("Task_queue is empty, try again...")
                continue
            except urllib2.URLError, e:
                # we just skip it over
                try:
                    self.db_cursor.execute(Spider.INSERT_RECORD_TO_FAILED_URL_TABLE,
                            (url, depthlevel, str(e)))
                except:
                    logging.warn("Insert failed-open url to DB error")

                self.task_queue.task_done()
                self.process_counter.fail()
                logging.warn("Error occured when trying to open the link: %s" % url)
                continue
            except Exception, e:
                # In most cases, BeautifulSoup operations raise this exception.
                # we just skip it over, too.
                self.task_queue.task_done()
                self.process_counter.fail()
                logging.warn("Something wrong happened: %s" % e)
                continue


    def url_has_any_extension(self, url, extensions):
        """Filter any links with extensions in 'extention' variable."""
        directory, filename = posixpath.splitext(urlparse.urlparse(url).path)
        if not filename:
            return False
        return filename[1:].lower() in extensions

    def dismiss(self):
        """Sets a flag to tell the thread to exit when done with current task."""
        self._dismissed.set()


class Spider():
    """A spider using threadpool, distributing task to its worker threads.

    Basic usage:
    >>> spider = Spider("http://www.baidu.com")
    >>> spider.wait()

    """

    CREATE_MATCH_PAGE_TABLE = "CREATE TABLE match_pages (url TEXT,dp_level INTEGER, content TEXT)"
    INSERT_RECORD_TO_MATCH_TABLE = "INSERT INTO match_pages VALUES(?, ?, ?)"
    CREATE_FAILED_URL_TABLE = "CREATE TABLE fail_urls (url TEXT, dp_level INTEGER, error_msg TEXT)"
    INSERT_RECORD_TO_FAILED_URL_TABLE = "INSERT INTO fail_urls VALUES(?, ?, ?)"
    HASH_TABLE_SIZE = 1048576   #2^24 = 1048576; 2^24=16777216

    def __init__(self, init_url, thread_num = 10, db_file='spider.py', depth=1,
            keyword=None, poll_time_out=5, queue_size=0):
        """Set up thread pool and start thread_num worker threads.

        thread_num:     the number of worker threads
        init_url:       valid URL to start with
        db_file:        sqlite3 database file to record pages match the keyword
        depth:          crawling depth from the init_url
        keyword:        default None
        poll_time_out:  interval in seconds to get task from the taskQueue
        queue_size:     upbound limit on the number of items that can be placed in the queue, 0 means infinite.

        """
        self.init_url = init_url
        self._thread_num = thread_num
        self.db_file = db_file
        self.depth = depth
        self.keyword_re = None
        if keyword:
            self.keyword_re = re.compile(ur'.*%s.*' % keyword.decode('utf8'), re.UNICODE)
        self._poll_time_out = poll_time_out
        self._taskQueue = Queue.Queue(queue_size)
        self._hash_table = bitarray(2**24)
        self._hash_table_lock = threading.Lock()
        self._db_connect = None
        self.workers = []
        self.processlogger = None
        self.process_counter = Counter()

        self.initDB()
        self.start_log_process()
        #enqueue initial url, 0 means the depth of init_url.
        self._taskQueue.put((self.init_url, 0))
        self.createWorker()

    def initDB(self):
        """Create db_file, initialize connection and create table. """
        # isolation_level=None means initialize connection with autocommit mode.
        self._db_connect = sqlite3.connect(self.db_file, isolation_level=None, check_same_thread=False)
        self._db_connect.text_factory = str
        logging.info("Initialize DB connection successful.")
        db_cursor = self._db_connect.cursor()
        db_cursor.execute(Spider.CREATE_MATCH_PAGE_TABLE)
        db_cursor.execute(Spider.CREATE_FAILED_URL_TABLE)
        self._db_connect.commit()
        db_cursor.close()
        logging.info("Create table done.")

    def start_log_process(self):
        self.processlogger = threading.Thread(target=self.printProcessInfo)
        self.processlogger.daemon = True
        self.processlogger.start()

    def printProcessInfo(self):
        while True:
            time.sleep(10)
            print "Remain task in queue: %7d, Successful parsed: %7d, Fail count: %3d, Bingo Count: %4d"\
                    % (self._taskQueue.qsize(), self.process_counter.success_count(),
                    self.process_counter.fail_count(), self.process_counter.bingo_count())

    def createWorker(self):
        """Add thread_num worker threads to the pool. """
        for _ in range(self._thread_num):
            self.workers.append(Worker(self._taskQueue, self.depth,
                self._hash_table, self._hash_table_lock, self._db_connect,
                self.process_counter, self.keyword_re, self._poll_time_out))
        logging.info("Created %d worker threads to the pool" % self._thread_num)

    def dismissWorkers(self):
        """Tell all worker threads to quit after their current task. """
        dismiss_list = []
        worker_num = len(self.workers)
        #dismiss all worder threads
        for _ in range(worker_num):
            worker = self.workers.pop()
            worker.dismiss()
            dismiss_list.append(worker)
        logging.info("All worker threads dismissed.")

        #join all dismissed threads
        for worker in dismiss_list:
            worker.join()
        logging.info("Joined all dismissded worker threads.")

    def wait(self):
        """Wait for all task in task queue have been done.

        dismiss and join all the worker threads, disconnect from DB.
        """
        self._taskQueue.join()
        self.dismissWorkers()
        self._db_connect.commit()
        self._db_connect.close()

def makeOptParser():
    parser = optparse.OptionParser()
    parser.add_option("-u", "--url", dest="url",
            help="initial URL to crawl from")
    parser.add_option("-d", "--depth", dest="depth", type="int",
            default=1, help="crawl depth from the initial URL")
    parser.add_option("-f", "--logfile", dest="logfile",
            default="spider.log", help="logfile name")
    parser.add_option("-l", "--loglevel", dest="loglevel", type="int",
            default=3, help="The numeric values of logging levels. The default level is 3(WARNING)")
    parser.add_option("--testself", dest="testself",
            action="store_false", default=False,
            help="run self test procedure.")
    parser.add_option("--threadnum", dest="threadnum", type="int",
            default=10, help="How many thread the Spider contains")
    parser.add_option("--dbfile", dest="dbfile", default="spider.db",
            help="sqlite db file to store web pages")
    parser.add_option("-k", "--key", dest="key",
            help="keyword to match while crawling webpages")
    return parser;

def main():
    parser = makeOptParser()
    (options, args) = parser.parse_args()

    logging.basicConfig(filename=options.logfile,
            format='%(asctime)s --%(threadName)s-- %(message)s',
            level=UPMOST_LOG_LEVEL-(options.loglevel-1)*10)
    spider = Spider(options.url, options.threadnum, options.dbfile, options.depth,
            options.key)
    spider.wait()

if __name__ == "__main__":
    main()
