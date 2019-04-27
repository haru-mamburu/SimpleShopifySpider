# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import time
import base64

class ShopifyPipeline(object):
    def process_item(self, item, spider):
        if item['type'] is not None and item['data'] is not None:
            # Catalog list item
            if 'catalogList' == item['type']:
                catalog_variants = item['data']
                with spider.db_connector.cursor() as cursor:
                    # Check if catalog existing
                    sql = ('SELECT CatalogName FROM tCatalogs '
                        'WHERE ShopID=%s AND CatalogName=%s AND CatalogLink=%s AND CatalogImage=%s;')
                    cursor.execute(
                        sql, 
                        (str(spider.shopid), 
                        str(catalog_variants['catalogName']), 
                        str(catalog_variants['catalogLink']), 
                        str(catalog_variants['imageLink']))
                        )
                    result_rows = cursor.fetchall()
                    bFound = False
                    for result_row in result_rows:
                        if len(result_row['CatalogName']) > 0:
                            bFound = True
                            spider.logger.info(msg='catalog<%s> has been crawled before' % result_row['CatalogName'])
                            break;
                    # Insert if not found
                    if bFound is not True:
                        sql = ('REPLACE INTO tCatalogs('
                            'ShopID,CatalogName,CatalogLink,ImageLink,ImageTimestamp,FirstCrawlingTime,LastCrawlingTime,CrawlingCounter) '
                            'VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),1);')
                        cursor.execute(
                            sql, 
                            (str(spider.shopid), 
                            str(catalog_variants['catalogName']), 
                            str(catalog_variants['catalogLink']), 
                            str(catalog_variants['imageLink']), 
                            str(catalog_variants['imageTimestamp']))
                            )
                        spider.db_connector.commit()
                        spider.total_catalog_number = spider.total_catalog_number + 1
                    # Update if found
                    else:
                        sql = ('UPDATE tCatalogs '
                            'SET CrawlingCounter=CrawlingCounter+1,LastCrawlingTime=CURRENT_TIMESTAMP() '
                            'WHERE ShopID=%s AND CatalogName=%s AND CatalogLink=%s AND CatalogImage=%s;')
                        cursor.execute(
                            sql, 
                            (str(spider.shopid), 
                            str(catalog_variants['catalogName']), 
                            str(catalog_variants['catalogLink']), 
                            str(catalog_variants['imageLink']))
                            )
                        spider.db_connector.commit()
            
            # Product list item
            elif 'productList' == item['type']:
                pass
            # Product detail item
            elif 'productDetails' == item['type']:
                product_variants = item['data']
                with spider.db_connector.cursor() as cursor:
                    # Check if product exiting
                    sql = ('SELECT ProductID FROM tProducts '
                        'WHERE ShopID=%s AND ProductID=%s AND '
                        'ProductName=%s AND CatalogName=%s AND '
                        'ProductPrice=%s AND ComparePrice=%s AND '
                        'ProductLink=%s AND ProductDescription=%s;')
                    cursor.execute(
                        sql, 
                        (str(spider.shopid), 
                        str(product_variants['productId']), 
                        str(product_variants['productName']), 
                        str(product_variants['category']), 
                        str(product_variants['productPrice']), 
                        str(product_variants['comparePrice']), 
                        str(product_variants['productLink']), 
                        str(product_variants['productDescription']))
                        )
                    result_rows = cursor.fetchall()
                    bFound = False
                    for result_row in result_rows:
                        if len(result_row['ProductID']) > 0:
                            bFound = True
                            spider.logger.info(msg='product<%s> has been crawled before' % result_row['ProductID'])
                            break;
                    # Insert if not found
                    if bFound is not True:
                        sql = ('REPLACE INTO tProducts('
                            'ShopID,ProductID,ProductName,CatalogName,ProductPrice,ComparePrice,ProductLink,ProductDescription,'
                            'VariantID,Variant,SKU,Brand,HotSale,FirstCrawlingTime,LastCrawlingTime,CrawlingCounter) '
                            'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'
                            '%s,%s,%s,%s,%s,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),1);')
                        cursor.execute(
                            sql, 
                            (
                            str(spider.shopid), 
                            str(product_variants['productId']), 
                            str(product_variants['productName']), 
                            str(product_variants['category']), 
                            str(product_variants['productPrice']), 
                            str(product_variants['comparePrice']), 
                            str(product_variants['productLink']), 
                            str(product_variants['productDescription']), 
                            str(product_variants['variantId']), 
                            str(product_variants['variant']), 
                            str(product_variants['sku']), 
                            str(product_variants['brand']), 
                            str(product_variants['hotSale'])
                            )
                            )
                        spider.db_connector.commit()
                        spider.total_product_number = spider.total_product_number + 1
                    # Update if found
                    else:
                        sql = ('UPDATE tProducts '
                            'SET CrawlingCounter=CrawlingCounter+1,LastCrawlingTime=CURRENT_TIMESTAMP() '
                            'WHERE ShopID=%s AND ProductID=%s AND '
                            'ProductName=%s AND CatalogName=%s AND '
                            'ProductPrice=%s AND ComparePrice=%s AND '
                            'ProductLink=%s AND ProductDescription=%s;')
                        cursor.execute(
                            sql, 
                            (str(spider.shopid), 
                            str(product_variants['productId']), 
                            str(product_variants['productName']), 
                            str(product_variants['category']), 
                            str(product_variants['productPrice']), 
                            str(product_variants['comparePrice']), 
                            str(product_variants['productLink']), 
                            str(product_variants['productDescription']))
                            )
                        spider.db_connector.commit()
                    
                    # Images ==> tProductImages
                    for image_item in product_variants['productImages']:
                        sql = ('REPLACE INTO tProductImages('
                            'ShopID,ProductID,ImageURL,ImageTimestamp,CrawlingTime) '
                            'VALUES(%s,%s,%s,%s,CURRENT_TIMESTAMP());')
                        cursor.execute(
                            sql, 
                            (str(spider.shopid), 
                            str(product_variants['productId']), 
                            str(image_item['imageLink']), 
                            str(image_item['imageTimestamp']))
                            )
                        spider.db_connector.commit()
                        
