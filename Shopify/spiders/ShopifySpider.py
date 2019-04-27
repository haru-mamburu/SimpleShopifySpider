# -*- coding: utf-8 -*-
import time, random
import sys, os, platform
import json
import pymysql.cursors
import scrapy
import base64
from Shopify.items import ShopifyItem
from scrapy.utils.project import get_project_settings

reload(sys) 
sys.setdefaultencoding('utf8')

# Start Command: scrapy crawl ShopifySpider -a shop=http://www.domain.com
class ShopifyspiderSpider(scrapy.Spider):
    name = 'ShopifySpider'
    allowed_domains = []
    start_urls = []
    catalog_list_selectors = []
    product_list_selectors = []
    product_detail_selectors = []
    existing_product_ids = []
    scrapied_product_ids = []
    catalog_list_selector_id = 0
    product_list_selector_id = 0
    product_detail_selector_id = 0
    shopid = 0
    shop_url = ''
    real_shopid = '0'
    admin_url = ''
    shop_theme = {}
    min_interval = 0
    max_interval = 0
    total_catalog_number = 0
    total_product_number = 0
    
    # Class construction
    def __init__(self, shop):
        # Inputs
        self.logger.info(msg='Ready to crawl shop (%s)' % (shop))
        self.time_counter = time.time()
        
        # Project settings
        self.settings = get_project_settings()
        if os.access(self.settings.get('MY_CONFIG_FILE'), os.F_OK):
            with open(self.settings.get('MY_CONFIG_FILE'), 'r') as fb:
                self.config_parameters = json.loads(fb.read())
        else:
            self.logger.info(msg='Please set valid config file(%s)' % self.settings.get('MY_CONFIG_FILE'))
            sys.exit()
        min_interval = float(self.config_parameters['times']['requestInterval'])
        max_interval = float(self.config_parameters['times']['requestInterval']) + 2
        
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
                    
        # Read parameters from database
        with self.db_connector.cursor() as cursor:
            # Read shop id and URL
            if '/' == shop[-1]:
                shop = shop[: -1]
            sql = 'SELECT ShopID,ShopURL,CrawlingState FROM tShops WHERE ShopURL LIKE \'%s%%\';' % shop
            cursor.execute(sql)
            result_rows = cursor.fetchall()
            crawl_state = 0
            for result_row in result_rows:
                self.shopid = int(result_row['ShopID'])
                self.shop_url = result_row['ShopURL']
                if '/' == self.shop_url[-1]:
                    self.shop_url = self.shop_url[0: -1]
                domain = self.parse_domain(self.shop_url)
                crawl_state = int(result_row['CrawlingState'])
                self.allowed_domains.append(domain)
            # self.logger.info(msg='shopid=%d, shop_url=%s, domain=%s' % (self.shopid, self.shop_url, self.allowed_domains))
            
            # Update crawl state
            if 0 == crawl_state:
                sql = 'UPDATE tShops SET FirstCrawlingTime=CURRENT_TIMESTAMP(),CrawlingState=1 WHERE ShopID=%s;' % str(self.shopid)
            else:
                sql = 'UPDATE tShops SET CrawlingState=1 WHERE ShopID=%s;' % str(self.shopid)
            cursor.execute(sql)
            self.db_connector.commit()
            
            # Read selector rules
            sql = 'SELECT SelectorID,SelectorType,SelectorRules FROM tSelectorRules;';
            cursor.execute(sql)
            result_rows = cursor.fetchall()
            for result_row in result_rows:
                if result_row['SelectorID'] is None or result_row['SelectorType'] is None or result_row['SelectorRules'] is None:
                    continue
                selector = json.loads(result_row['SelectorRules'])
                selector['id'] = int(result_row['SelectorID'])
                if 'catalog.list' == result_row['SelectorType']:
                    self.catalog_list_selectors.append(selector)
                elif 'product.list' == result_row['SelectorType']:
                    self.product_list_selectors.append(selector)
                elif 'product.details' == result_row['SelectorType']:
                    self.product_detail_selectors.append(selector)
            # self.logger.info(msg='catalog_list_selectors=%s' % self.catalog_list_selectors)
            # self.logger.info(msg='product_list_selectors=%s' % self.product_list_selectors)
            # self.logger.info(msg='product_detail_selectors=%s' % self.product_detail_selectors)
            
            # Read existing product ids
            sql = 'SELECT ProductID FROM tProducts WHERE ShopID=%d;' % self.shopid
            cursor.execute(sql)
            result_rows = cursor.fetchall()
            for result_row in result_rows:
                if result_row['ProductID'] is not None and '' != result_row['ProductID']:
                    self.existing_product_ids.append(int(result_row['ProductID']))
            # self.logger.info(msg='existing_product_ids=%s' % self.existing_product_ids)
        
        # Initialize parent class
        super(ShopifyspiderSpider, self).__init__()
        
    # Closed
    def closed(self, reason):
        self.logger.info(msg='Updating and cleaning before exit')
        # Update tShops
        with self.db_connector.cursor() as cursor:
            sql = ('UPDATE tShops '
                    'SET RealShopID=%s,ShopTitle=%s,ShopDomain=%s,AdminURL=%s,'
                    'ShopThemeID=%s,ShopThemeName=%s,'
                    'CatalogListSelectorID=%s,ProductListSelectorID=%s,ProductDetailSelectorID=%s,'
                    'LastCrawlingTime=CURRENT_TIMESTAMP(),CrawlingCounter=CrawlingCounter+1,CrawlingState=2 '
                    'WHERE ShopID=%s;')
            cursor.execute(
                    sql, 
                    (
                    self.real_shopid, 
                    self.shop_title, 
                    self.allowed_domains[0], 
                    self.admin_url, 
                    self.shop_theme['id'], 
                    self.shop_theme['name'], 
                    str(self.catalog_list_selector_id), 
                    str(self.product_list_selector_id), 
                    str(self.product_detail_selector_id), 
                    str(self.shopid)
                    )
                    )
            self.db_connector.commit()
            
        # Close database connector
        if self.db_connector is not None:
            self.db_connector.close()
        
        self.logger.info(
            msg='it took %d seconds to crawl shop (%s): %d products from %d catalogs' % 
            (time.time() - self.time_counter, self.shop_url, self.total_product_number, self.total_catalog_number))
    
    # Start request
    def start_requests(self):
        yield scrapy.Request(self.shop_url, self.parse_home)
        time.sleep(random.uniform(self.min_interval, self.max_interval))
        yield scrapy.Request(self.shop_url + '/collections', self.parse_catalogs)

    # Entrance of home page parser:
    # 1) Request product details of hot-sale, callback=parse_product_details
    def parse_home(self, response):
        # parse shop variants
        self.parse_shop_variants(response)
        
        # parse products
        self.parse_catalog_products(response, 'Home')
        
    # 1) Parse catalogs
    # 2) Request products of catalog, callback=parse_catalog_products
    def parse_catalogs(self, response):
        # parse shop variants
        self.parse_shop_variants(response)
        
        # parse catalog titles/links array
        catalog_titles = []
        catalog_links = []
        catalog_images = []
        for selector in self.catalog_list_selectors:
            if 'id' not in selector.keys() or 'type' not in selector.keys() or 'rules' not in selector.keys():
                continue
            catalog_titles = []
            catalog_links = []
            catalog_images = []
            titles_selector = selector['rules'].get('titles')
            links_selector = selector['rules'].get('links')
            images_selector = selector['rules'].get('images')
            if 'css' == selector['type']:
                catalog_titles = response.css(titles_selector).extract()
                catalog_links = response.css(links_selector).extract()
                if images_selector is not None and '' != images_selector:
                    catalog_images = response.css(images_selector).extract()
            elif 'xpath' == selector['type']:
                catalog_titles = response.xpath(titles_selector).extract()
                catalog_links = response.xpath(links_selector).extract()
                if images_selector is not None and '' != images_selector:
                    catalog_images = response.xpath(images_selector).extract()
            if catalog_titles is not None and catalog_links is not None and len(catalog_titles) == len(catalog_links) and len(catalog_titles) > 0:
                if len(catalog_images) <= 0:
                    for i in range(0, len(catalog_titles)):
                        catalog_images.append('')
                self.catalog_list_selector_id = int(selector['id'])
                self.logger.info(msg='catalog_list_selector_id=%d' % (self.catalog_list_selector_id))
                # self.logger.info(msg='catalog_titles=%s, catalog_links=%s, catalog_images=%s' % (catalog_titles, catalog_links, catalog_images))
                break;
            
        # parse catalog title/link one by one
        for title, link, image in zip(catalog_titles, catalog_links, catalog_images):
            title = title.replace('\n', '').strip()
            image_link = ''
            image_timestamp = ''
            # background-image: url('ImagePath?v=1536316560');
            if image.find('url(') >= 0:
                begin_pos = image.find('url(')
                end_pos = image.find(')', begin_pos + len('url('))
                if begin_pos >= 0 and end_pos > begin_pos:
                    image = image[begin_pos + len('url('): end_pos]
                    image = image.replace('\'', '').replace('\'', '')
            # ImagePath?v=1530950057 ==> ImagePath
            begin_pos = image.find('?')
            if begin_pos >= 0:
                image_link = image[0: begin_pos]
                image_timestamp = image[begin_pos + 1: ].replace('v', '').replace('=', '').replace(' ', '')
            if '' == title:
                title = link.split('')[-1]
            if '' != link and '/' == link[0]:
                link = 'https://' + self.allowed_domains[0] + link
            else:
                link = 'https://' + self.allowed_domains[0] + '/' + link
            if image_link.find('https') < 0:
                if image_link.find('//') >= 0:
                    image_link = 'https:' + image_link
                else:
                    image_link = 'https://' + image_link
            
            # Post pipeline item
            catalog_item = ShopifyItem()
            catalog_item['type'] = 'catalogList'
            catalog_item['data'] = {'catalogName':title,'catalogLink':link,'imageLink':image_link,'imageTimestamp':image_timestamp}
            # self.logger.info(msg='catalogList.type=%s, catalogList.data=%s' % (catalog_item['type'], catalog_item['data']))
            yield catalog_item
            
            # Yield catalog products request
            time.sleep(random.uniform(self.min_interval, self.max_interval))
            yield scrapy.Request(link, callback = lambda response, catalog = title : self.parse_catalog_products(response, catalog))
        
    # 1) Parse products of catalog
    # 2) Request product details, callback=parse_product_details
    def parse_catalog_products(self, response, catalog):
        # parse product list
        product_titles = []
        product_links = []
        product_images = []
        for selector in self.product_list_selectors:
            if 'id' not in selector.keys() or 'type' not in selector.keys() or 'rules' not in selector.keys():
                continue
            product_titles = []
            product_links = []
            product_images = []
            titles_selector = selector['rules'].get('titles')
            links_selector = selector['rules'].get('links')
            images_selector = selector['rules'].get('images')
            if 'css' == selector['type']:
                product_titles = response.css(titles_selector).extract()
                product_links = response.css(links_selector).extract()
                if images_selector is not None and '' != images_selector:
                    product_images = response.css(images_selector).extract()
            elif 'xpath' == selector['type']:
                product_titles = response.xpath(titles_selector).extract()
                product_links = response.xpath(links_selector).extract()
                if images_selector is not None and '' != images_selector:
                    product_images = response.xpath(images_selector).extract()
            if product_titles is not None and product_links is not None and len(product_titles) == len(product_links) and len(product_titles) > 0:
                if len(product_images) <= 0:
                    for i in range(0, len(product_titles)):
                        product_images.append('')
                self.product_list_selector_id = int(selector['id'])
                self.logger.info(msg='product_list_selector_id=%d' % (self.product_list_selector_id))
                # self.logger.info(msg='product_titles=%s, product_links=%s, product_images=%s' % (product_titles, product_links, product_images))
                break;
            
        # parse product title/link one by one
        for title, link, image in zip(product_titles, product_links, product_images):
            title = title.replace('\n', '').strip()
            image_link = ''
            image_timestamp = ''
            # background-image: url('ImagePath?v=1536316560');
            if image.find('url(') >= 0:
                begin_pos = image.find('url(')
                end_pos = image.find(')', begin_pos + len('url('))
                if begin_pos >= 0 and end_pos > begin_pos:
                    image = image[begin_pos + len('url('): end_pos]
                    image = image.replace('\'', '').replace('\'', '').replace(' ', '')
            # ImagePath?v=1530950057 ==> ImagePath
            begin_pos = image.find('?')
            if begin_pos >= 0:
                image_link = image[0: begin_pos]
                image_timestamp = image[begin_pos + 1: ].replace('v', '').replace('=', '').replace(' ', '')
            if '' == title:
                title = link.split('')[-1]
            if '' != link and '/' == link[0]:
                link = 'https://' + self.allowed_domains[0] + link
            else:
                link = 'https://' + self.allowed_domains[0] + '/' + link
            if image_link.find('https') < 0:
                if image_link.find('//') >= 0:
                    image_link = 'https:' + image_link
                else:
                    image_link = 'https://' + image_link
            
            # Post pipeline item
            product_list_item = ShopifyItem()
            product_list_item['type'] = 'productList'
            product_list_item['data'] = {'productName':title,'productLink':link,'imageLink':image_link,'imageTimestamp':image_timestamp}
            # self.logger.info(msg='productList.type=%s, productList.data=%s' % (product_list_item['type'], product_list_item['data']))
            yield product_list_item
            
            # Yield catalog products request
            time.sleep(random.uniform(self.min_interval, self.max_interval))
            yield scrapy.Request(link, callback = lambda response, catalog = catalog : self.parse_product_details(response, catalog))
        
    # Parse product details
    def parse_product_details(self, response, catalog):
        # parse product variants
        product_variants = self.parse_product_variants(response, catalog)
        # parse product details
        product_found = False
        for selector in self.product_detail_selectors:
            product_title_selector = selector['rules'].get('productTitle')
            compare_price_selector = selector['rules'].get('comparePrice')
            product_price_selector = selector['rules'].get('productPrice')
            product_desc_selector = selector['rules'].get('productDescription')
            product_images_selector = selector['rules'].get('productImages')
            # self.logger.info(msg='product_title_selector=%s, product_price_selector=%s, compare_price_selector=%s' % (product_title_selector, product_price_selector, compare_price_selector))
            if selector['type'] is None or '' == selector['type']:
                continue
            if product_title_selector is None or '' == product_title_selector:
                continue
            if compare_price_selector is None or len(compare_price_selector) <= 0:
                continue
            if product_price_selector is None or len(product_price_selector) <= 0:
                continue
            if product_desc_selector is None or '' == product_desc_selector:
                continue
            if product_images_selector is None or '' == product_images_selector:
                continue
            if 'css' == selector['type']:
                product_variants['productName'] = response.css(product_title_selector).extract_first()
                if product_variants['productName'] is not None:
                    product_variants['productName'] = product_variants['productName'].replace('\n', '').strip()
                else:
                    product_variants['productName'] = ''
                for price_selector in compare_price_selector:
                    product_variants['comparePrice'] = response.css(price_selector).extract_first()
                    if product_variants['comparePrice'] is not None:
                        product_variants['comparePrice'] = product_variants['comparePrice'].replace('\n', '').strip()
                        if len(product_variants['comparePrice']) > 0:
                            break
                if product_variants['comparePrice'] is None:
                    product_variants['comparePrice'] = ''
                for price_selector in product_price_selector:
                    product_variants['productPrice'] = response.css(price_selector).extract_first()
                    if product_variants['productPrice'] is not None:
                        product_variants['productPrice'] = product_variants['productPrice'].replace('\n', '').strip()
                        if len(product_variants['productPrice']) > 0:
                            break
                if product_variants['productPrice'] is None:
                    product_variants['productPrice'] = ''
                product_desc_list = response.css(product_desc_selector).extract()
                product_desc_text = ''
                for product_desc in product_desc_list:
                    if product_desc is not None and len(product_desc) > 0:
                        product_desc_text += product_desc
                product_variants['productDescription'] = base64.b64encode(product_desc_text)
                product_images = response.css(product_images_selector).extract()
            elif 'xpath' == selector['type']:
                product_variants['productName'] = response.xpath(product_title_selector).extract_first()
                if product_variants['productName'] is not None:
                    product_variants['productName'] = product_variants['productName'].replace('\n', '').strip()
                else:
                    product_variants['productName'] = ''
                for price_selector in compare_price_selector:
                    product_variants['comparePrice'] = response.xpath(price_selector).extract_first()
                    if product_variants['comparePrice'] is not None:
                        product_variants['comparePrice'] = product_variants['comparePrice'].replace('\n', '').strip()
                        if len(product_variants['comparePrice']) > 0:
                            break
                if product_variants['comparePrice'] is None:
                    product_variants['comparePrice'] = ''
                for price_selector in product_price_selector:
                    product_variants['productPrice'] = response.xpath(price_selector).extract_first()
                    if product_variants['productPrice'] is not None:
                        product_variants['productPrice'] = product_variants['productPrice'].replace('\n', '').strip()
                        if len(product_variants['productPrice']) > 0:
                            break
                if product_variants['productPrice'] is None:
                    product_variants['productPrice'] = ''
                product_desc_list = response.xpath(product_desc_selector).extract()
                product_desc_text = ''
                for product_desc in product_desc_list:
                    if product_desc is not None and len(product_desc) > 0:
                        product_desc_text += product_desc
                product_variants['productDescription'] = base64.b64encode(product_desc_text)
                product_images = response.xpath(product_images_selector).extract()
            product_variants['productImages'] = []
            for product_image in product_images:
                # //cdn.shopify.com/s/files/1/2487/8134/products/18_300x.PNG?v=1545378180
                if '' == product_image:
                    continue
                image_link = ''
                image_timestamp = ''
                image_values = product_image.split('?')
                if len(image_values) >= 2:
                    image_link = image_values[0]
                    image_timestamp = image_values[1].replace('v', '').replace('=', '').replace(' ', '')
                else:
                    image_link = image_values[0]
                if image_link.find('https') < 0:
                    if image_link.find('//') >= 0:
                        image_link = 'https:' + image_link
                    else:
                        image_link = 'https://' + image_link
                product_variants['productImages'].append({'imageLink':image_link,'imageTimestamp':image_timestamp})
            # break selectors loop
            if product_variants['productName'] is not None and len(product_variants['productName']) > 0 and product_variants['productPrice'] is not None and len(product_variants['productPrice']) > 0:
                self.product_detail_selector_id = int(selector['id'])
                self.logger.info(msg='product_detail_selector_id=%d' % (self.product_detail_selector_id))
                product_found = True
                break
         
        # Yield product item
        if product_found:
            product_variants['productLink'] = response.url
            if int(product_variants['productId']) not in self.scrapied_product_ids:
                self.scrapied_product_ids.append(int(product_variants['productId']))
            product_details_item = ShopifyItem()        
            product_details_item['type'] = 'productDetails'
            product_details_item['data'] = product_variants
            # self.logger.info(msg='productId=%s, productName=%s, productPrice=%s, comparePrice=%s, productImages=%s' % (str(product_variants['productId']), product_variants['productName'], product_variants['productPrice'], product_variants['comparePrice'], product_variants['productImages']))
            yield product_details_item
        
    # parse domain
    def parse_domain(self, url):
        # Input = http(s)://www.xxx.zzz or http(s)://www.xxx.zzz/yyy or www.xxx.zzz
        domain = ''
        start_index = url.find('//', 0)
        if start_index >= 0:
            end_index = url.find('/', start_index + 2)
            if end_index > start_index:
                domain = url[start_index + 2: end_index]
            else:
                domain = url[start_index + 2: ]
        else:
            end_index = url.find('/', 0)
            if end_index >= 0:
                domain = url[0: end_index]
            else:
                domain = url[:]
        return domain
        
    # parse response shop variants
    def parse_shop_variants(self, response):
        # shop id
        shopify_features = response.css('#shopify-features').extract_first()
        if shopify_features is not None:
            begin_pos = shopify_features.find('>')
            end_pos = shopify_features.find('<', begin_pos + 1)
            if begin_pos >= 0 and end_pos > begin_pos:
                json_str = shopify_features[begin_pos + 1: end_pos]
                if json_str is not None and '' != json_str:
                    json_object = json.loads(json_str);
                    self.real_shopid = json_object['shopId']
        
        # shop admin url
        body_content = str(response.body)
        begin_pos = body_content.find('Shopify.shop')
        end_pos = body_content.find(';', begin_pos + len('Shopify.shop'))
        if begin_pos >= 0 and end_pos > begin_pos:
            admin_str = body_content[begin_pos + len('Shopify.shop'): end_pos]
            if admin_str is not None and '' != admin_str:
                begin_pos = admin_str.find(''')
                end_pos = admin_str.find(''', begin_pos + 1)
                if begin_pos >= 0 and end_pos > begin_pos:
                    self.admin_url = admin_str[begin_pos + 1: end_pos]
                    if self.admin_url.find('https://') < 0:
                        self.admin_url = 'https://' + self.admin_url
                        
        # shop theme
        begin_pos = body_content.find('Shopify.theme')
        end_pos = body_content.find(';', begin_pos + len('Shopify.theme'))
        if begin_pos >= 0 and end_pos > begin_pos:
            theme_str = body_content[begin_pos + len('Shopify.theme'): end_pos]
            if theme_str is not None and '' != theme_str:
                begin_pos = theme_str.find('{')
                end_pos = theme_str.find('}', begin_pos + 1)
                if begin_pos >= 0 and end_pos > begin_pos:
                    theme_str = theme_str[begin_pos: end_pos + 1]
                    self.shop_theme = json.loads(theme_str)
                        
        # shop title
        self.shop_title = self.allowed_domains[0]
        title = response.css('title::text').extract_first().replace('\n','').lstrip().rstrip()
        begin_pos = title.find('–')
        if begin_pos >= 0:
            title = title[begin_pos + 1: ]
            self.shop_title = title.lstrip().rstrip()
         
        # Trace log
        # self.logger.info(msg='real_shopid=%s, shop_title=%s, admin_url=%s, shop_theme=%s' % (self.real_shopid, self.shop_title, self.admin_url, self.shop_theme))
                    
    # parse product variants
    def parse_product_variants(self, response, catalog):
        # Product variants
        product_variants = {
            'variantId':0,
            'productId':0,
            'name':'',
            'price':'',
            'currency':'',
            'sku':'',
            'brand':'',
            'variant':'',
            'category':'',
            'hotSale':0}
        body_content = str(response.body)
        begin_pos = body_content.find('window.ShopifyAnalytics.lib.track')
        if begin_pos >= 0:
            begin_pos = body_content.find('{', begin_pos + len('window.ShopifyAnalytics.lib.track'))
            end_pos = body_content.find('}', begin_pos + 1)
            if begin_pos >= 0 and end_pos > begin_pos:
                json_str = body_content[begin_pos: end_pos + 1]
                json_str = json_str.replace('/', '').replace('\\', '').replace('\n', '')
                # self.logger.info(msg='AnalyticsJson=%s' % json_str)
                if json_str is not None and len(json_str) > 0:
                    variants = json.loads(json_str)
                    if 'variantId' in variants.keys():
                        product_variants['variantId'] = variants['variantId']
                    if 'productId' in variants.keys():
                        product_variants['productId'] = variants['productId']
                    if 'name' in variants.keys():
                        product_variants['name'] = variants['name']
                    if 'price' in variants.keys():
                        product_variants['price'] = variants['price']
                    if 'currency' in variants.keys():
                        product_variants['currency'] = variants['currency']
                    if 'sku' in variants.keys():
                        product_variants['sku'] = variants['sku']
                    if 'brand' in variants.keys():
                        product_variants['brand'] = variants['brand']
                    if 'variant' in variants.keys():
                        product_variants['variant'] = variants['variant']
                    if 'category' in variants.keys():
                        product_variants['category'] = variants['category']
        # Check if product belongs to hot-sale catalog
        low_catalog = catalog.lower()
        if low_catalog.find('hot') >= 0 or low_catalog.find('best') >= 0 or low_catalog.find('trend') >= 0 or low_catalog.find('fashion') >= 0:
            product_variants['hotSale'] = 1
        if '' == product_variants['category']:
            product_variants['category'] = catalog
                
        return product_variants