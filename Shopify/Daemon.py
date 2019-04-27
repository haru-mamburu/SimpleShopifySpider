#!/root/anaconda2/bin/python
# -*- coding: utf-8 -*-

import sys, os, platform
import scrapy
import time, datetime
import logging
import json
import pymysql.cursors

reload(sys) 
sys.setdefaultencoding('utf8')

MY_CONFIG_FILE = '/root/eCommerceSpiders/Config/Spider.conf'

# cd /root/eCommerceSpiders/Shopify/Shopify && python Daemon.py > /dev/null 2>&1 &
# ps -ef | grep scrapy | grep -v grep | awk '{print $2}' | xargs kill -9
class ShopifySpiderDaemon:
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
        # Stop scrapy
        os.system('ps -ef | grep scrapy | grep -v grep | awk \'{print $2}\' | xargs kill -9');
        
        # Close database connector
        if self.db_connector is not None:
            self.logger.info('close database connector before exit')
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
            shopify_spiders = []
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
                    shopify_spiders.append(shop)
            self.logger.info('%d spiders were ready to start' % len(shopify_spiders))
            
            # Run spider one by one
            for shop in shopify_spiders:
                if self.check_shop_spider(shop) is True:
                    cmd_line = 'scrapy crawl ShopifySpider -a shop=%s' % shop['shop_url']
                    os.system(cmd_line)
                time.sleep(3)
                
            # Check spider every 10 minutes
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
    
if __name__ == '__main__':
    ShopifySpiderDaemon().run()