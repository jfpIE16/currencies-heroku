# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

# Datetime options
from datetime import date

class CurrencyexchangegtPipeline:

    def __init__(self):
        import psycopg2
        self.conn = psycopg2.connect(
            user="kcvkmymglaalmz",
            dbname="d90bkhs142ppg0",
            host="ec2-52-0-93-3.compute-1.amazonaws.com",
            password="a3c18a78f9a33dfa9b2dabe8f9e9bc4e3f8e9609bd70f8422ba07a00754add02"
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
