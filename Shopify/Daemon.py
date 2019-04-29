#!/root/anaconda2/bin/python
# -*- coding: utf-8 -*-

import sys, os, platform
import scrapy
import time, datetime
import logging
import json
import pymysql.cursors
import multiprocessing

reload(sys) 
sys.setdefaultencoding('utf8')

MY_CONFIG_FILE = '/root/eCommerceSpiders/Config/Spider.conf'

# cd /root/eCommerceSpiders/Shopify/Shopify && python Daemon.py > /dev/null 2>&1 &
# ps -ef | grep scrapy | grep -v grep | awk '{print $2}' | xargs kill -9

class ShopifySpiderDaemon:
    sub_processes = []
    
    # Class construction
    def __init__(self):
        # Initialize Logger
        log_path = os.path.join(os.getcwd(), 'Logs')
        if os.path.exists(log_path) is not True:
            os.mkdir(log_path)
        log_file = 'Daemon_' + time.strftime('%Y%m%d',time.localtime(time.time())) + '.LOG'
        log_path = os.path.join(log_path, log_file)
        log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.log_handler = logging.FileHandler(log_path)
        self.log_handler.setLevel(logging.INFO)
        self.log_handler.setFormatter(log_formatter)        
        self.logger = logging.getLogger('Daemon')
        self.logger.setLevel(level = logging.INFO)
        self.logger.addHandler(self.log_handler)
        
        # Project settings
        if os.access(MY_CONFIG_FILE, os.F_OK):
            with open(MY_CONFIG_FILE, 'r') as fb:
                self.config_parameters = json.loads(fb.read())
        else:
            self.logger.info('Please set valid config file(%s)' % MY_CONFIG_FILE)
            sys.exit()
        
        # Create database connector
        self.db_connector = None
        if 'mysql' == self.config_parameters['database']['type']:
            self.db_connector = pymysql.connect(
                host = str(self.config_parameters['database']['host']),
                port = int(self.config_parameters['database']['port']),
                user = str(self.config_parameters['database']['user']),
                passwd = str(self.config_parameters['database']['password']),
                db = str(self.config_parameters['database']['dbname']),
                charset = str(self.config_parameters['database']['charset']),
                cursorclass = pymysql.cursors.DictCursor)
        
    # Class destruction
    def __del__(self):
        self.logger.info('Free resources before exit')
        
        # Kill all sub processes
        for process in self.sub_processes:
            os.system('kill -9 %d' % process.pid);
        
        # Stop scrapy
        os.system("ps -ef | grep scrapy | grep -v grep | awk '{print $2}' | xargs kill -9");
        
        # Close database connector
        if self.db_connector is not None:
            self.db_connector.close()
        
    # Run spiders
    def run(self):
        last_run_time = 0
        while (True):
            # Check time to run spiders
            last_run_time = int(time.time()) - last_run_time
            if last_run_time < 600:
                time.sleep(10)
                continue
            last_run_time = int(time.time())
            self.logger.info('Ready to get and start spiders')
            
            # Getting all shopify spiders
            shopify_shops = []
            self.sub_processes = []
            with self.db_connector.cursor() as cursor:
                sql = ('SELECT ShopID,ShopURL,UNIX_TIMESTAMP(IFNULL(LastCrawlingTime,\'2000-01-01 00:00:00\')) AS LastCrawlingTime,CrawlingFrequency,CrawlingState '
                        'FROM tShops WHERE ShopType=1 AND CrawlingState!=1;')
                cursor.execute(sql)
                result_rows = cursor.fetchall()
                for result_row in result_rows:
                    shop = {
                        'shopid':int(result_row['ShopID']),
                        'shop_url':result_row['ShopURL'],
                        'last_crawling_time':int(result_row['LastCrawlingTime']),
                        'crawling_freq':int(result_row['CrawlingFrequency']),
                        'crawling_state':int(result_row['CrawlingState'])
                    }
                    if shop['crawling_freq'] <= 0:
                        shop['crawling_freq'] = 1800
                    
                    # Add spider cache
                    if self.check_shop_spider(shop) is True:
                        shopify_shops.append(shop)
                        
                    # Create depend sub process for 10 spiders
                    if len(shopify_shops) >= 10:
                        process = multiprocessing.Process(
                            target = self.start_scrapy_spiders, 
                            args = (shopify_shops[:], ))
                        process.start()
                        self.sub_processes.append(process)
                        shopify_shops = []

            # Remaider shops
            if len(shopify_shops) > 0:
                process = multiprocessing.Process(
                    target = self.start_scrapy_spiders, 
                    args = (shopify_shops[:], ))
                process.start()
                self.sub_processes.append(process)
            
            # Waiting for all sub processes quit
            for process in self.sub_processes:
                process.join()
                
            # Next checking
            time.sleep(10)
                
    # Check whether shop spider need start
    def check_shop_spider(self, shop):
        # Check parameters
        if shop['shopid'] is None or shop['shop_url'] is None or len(shop['shop_url']) <= 0:
            return False
        if shop['crawling_freq'] is None or shop['crawling_freq'] <= 0:
            return False
        if shop['last_crawling_time'] is None or shop['last_crawling_time'] <= 0:
            return True
        if shop['crawling_state'] is None or 0 == shop['crawling_state']:
            return True
        if 1 == shop['crawling_state']:
            return False
        
        # Check if time to start
        time_elipsed = int(time.time()) - shop['last_crawling_time']
        if time_elipsed >= shop['crawling_freq']:
            self.logger.info('Time to start spider (%s)' % shop)
            return True
            
        return False
        
    # Start scrapy spiders
    def start_scrapy_spiders(self, shops):
        for shop in shops:
            cmd_line = 'scrapy crawl ShopifySpider -a shop=%s' % shop['shop_url']
            os.system(cmd_line)
        

if __name__ == '__main__':
    ShopifySpiderDaemon().run()