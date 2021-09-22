# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

# Datetime options
from datetime import date

# Get env variables
import os

# DB Handler
import psycopg2

# URL parser
from urllib.parse import urlparse

class CurrencyexchangegtPipeline:

    def __init__(self):
        db_url = os.environ['DATABASE_URL']
        result = urlparse(db_url)
        self.conn = psycopg2.connect(
            user=result.username,
            dbname=result.path[1:],
            host=result.hostname,
            password=result.password
        )

    def process_item(self, item, spider):
        cursor = self.conn.cursor()
        today = date.today()
        query = f'''
        INSERT INTO tipo_cambio
        VALUES ('{item['Banco']}', {item['Compra']}, {item['Venta']}, '{today.strftime("%m/%d/%y")}')
        '''
        print(query)
        cursor.execute(query)
        self.conn.commit()
        return item
