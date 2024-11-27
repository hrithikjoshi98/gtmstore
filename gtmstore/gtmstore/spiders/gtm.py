import hashlib
from typing import Iterable
from urllib.parse import urlparse
import parsel
import scrapy
import json

from scrapy import Request
from scrapy.cmdline import execute
from gtmstore.items import GtmstoreItem
from gtmstore.db_config import config
import pymysql
import datetime
import os
import gzip
from parsel import Selector
import re


def remove_extra_space(row_data):
    # Remove any extra spaces or newlines created by this replacement
    value = re.sub(r'\s+', ' ', row_data).strip()
    # Update the cleaned value back in row_data
    return value


def generate_hashid(url: str) -> str:
    # Parse the URL and use the netloc and path as a unique identifier
    parsed_url = urlparse(url)
    unique_string = parsed_url.netloc + parsed_url.path
    # Create a hash of the unique string using SHA-256 and take the first 8 characters
    hash_object = hashlib.sha256(unique_string.encode())
    hashid = hash_object.hexdigest()[:8]  # Take the first 8 characters
    return hashid


def convert_schedule(input_str):
    # Mapping for days of the week
    days_of_week = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

    # Convert time format like 10am to 10:00
    def convert_time(time_str):
        # Match time and period (am/pm)
        match = re.match(r"(\d+)(am|pm)", time_str)
        if match:
            hour, period = match.groups()
            hour = int(hour)
            # Convert to 24-hour format
            if period == "am" and hour == 12:  # handle 12am as 00:00
                return "00:00"
            elif period == "pm" and hour != 12:  # handle pm times except for 12pm
                return f"{hour + 12}:00"
            return f"{hour:02}:00"
        return time_str  # If the format is wrong, return as is

    # Initialize the output schedule
    schedule = []

    # First process the "Monday thru Friday"
    m_f_times = re.search(r"Monday thru Friday: (\d+am) – (\d+pm)", input_str)
    if m_f_times:
        start_time = convert_time(m_f_times.group(1))
        end_time = convert_time(m_f_times.group(2))
        for i in range(1, 6):  # Monday to Friday
            schedule.append(f"{days_of_week[i]}: {start_time}-{end_time}pm")

    # Then process Saturday
    saturday_times = re.search(r"Saturday: (\d+am) – (\d+pm)", input_str)
    if saturday_times:
        start_time = convert_time(saturday_times.group(1))
        end_time = convert_time(saturday_times.group(2))
        schedule.append(f"{days_of_week[6]}: {start_time}-{end_time}")

    # Finally, process Sunday
    sunday_times = re.search(r"Sunday: (\d+am) – (\d+pm)", input_str)
    if sunday_times:
        start_time = convert_time(sunday_times.group(1))
        end_time = convert_time(sunday_times.group(2))
        schedule.append(f"{days_of_week[0]}: {start_time}-{end_time}")

    # Join the schedule into the final format
    return " | ".join(schedule).replace('pm', '').replace('am', '').strip()

class GtmSpider(scrapy.Spider):
    name = "gtm"
    start_urls = ["https://www.gtmstores.com/locations/"]

    def my_print(self, tu):
        for i in tu:
            print(i)
        print('\n')

    def __init__(self, start_id, end_id, **kwargs):
        super().__init__(**kwargs)
        self.start_id = start_id
        self.end_id = end_id

        self.conn = pymysql.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            db=config.database,
            autocommit=True
        )
        self.cur = self.conn.cursor()

        self.domain = self.start_urls[0].split('://')[1].split('/')[0]
        self.date = datetime.datetime.now().strftime('%d_%m_%Y')

        if 'www' in self.domain:
            self.sql_table_name = self.domain.split('.')[1].replace('-','_') + f'_{self.date}' + '_USA'
        else:
            self.sql_table_name = self.domain.split('.')[0].replace('-','_') + f'_{self.date}' + '_USA'
        self.folder_name = self.domain.replace('.', '_').strip()
        config.file_name = self.folder_name

        self.html_path = 'C:\page_source\\' + self.date + '\\' + self.folder_name + '\\'
        if not os.path.exists(self.html_path):
            os.makedirs(self.html_path)
        # print(self.domain, self.folder_name, self.sql_table_name)
        config.db_table_name = self.sql_table_name

        field_list = []
        value_list = []
        item = ('store_no', 'name', 'latitude', 'longitude', 'street', 'city',
                  'state', 'zip_code', 'county', 'phone', 'open_hours', 'url',
                  'provider', 'category', 'updated_date', 'country', 'status',
                  'direction_url', 'pagesave_path')
        for field in item:
            field_list.append(str(field))
            value_list.append('%s')
        config.fields = ','.join(field_list)
        config.values = ", ".join(value_list)

        self.cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.sql_table_name}(id int AUTO_INCREMENT PRIMARY KEY,
                                    store_no varchar(100) DEFAULT 'N/A',
                                    name varchar(100) DEFAULT 'N/A',
                                    latitude varchar(100) DEFAULT 'N/A',
                                    longitude varchar(100) DEFAULT 'N/A',
                                    street varchar(500) DEFAULT 'N/A',
                                    city varchar(100) DEFAULT 'N/A',
                                    state varchar(100) DEFAULT 'N/A',
                                    zip_code varchar(100) DEFAULT 'N/A',
                                    county varchar(100) DEFAULT 'N/A',
                                    phone varchar(100) DEFAULT 'N/A',
                                    open_hours varchar(500) DEFAULT 'N/A',
                                    url varchar(500) DEFAULT 'N/A',
                                    provider varchar(100) DEFAULT 'N/A',
                                    category varchar(100) DEFAULT 'N/A',
                                    updated_date varchar(100) DEFAULT 'N/A',
                                    country varchar(100) DEFAULT 'N/A',
                                    status varchar(100) DEFAULT 'N/A',
                                    direction_url varchar(500) DEFAULT 'N/A',
                                    pagesave_path varchar(500) DEFAULT 'N/A'
                                    )""")

    def start_requests(self):
        cookies = {
            '_fbp': 'fb.1.1732625462209.915517222532987878',
            '_gid': 'GA1.2.1731575346.1732625463',
            'hu-consent': '{"consent":true,"consentLevel":3,"consentID":"1cfd5dbc-1ab1-4368-a33a-51caddc06a93","categories":{"1":true,"2":true,"3":true,"4":true},"expiry":365,"timestamp":1732625470998,"blocking":{"blocked":0,"allowed":3,"blockedServices":[],"allowedServices":["2","34","58"]},"lastVersion":9}',
            '_ga_0B23PQ385B': 'GS1.1.1732703510.2.1.1732703732.0.0.0',
            '_ga': 'GA1.2.1143240425.1732625462',
        }

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,tr;q=0.8',
            'cache-control': 'no-cache',
            # 'cookie': '_fbp=fb.1.1732625462209.915517222532987878; _gid=GA1.2.1731575346.1732625463; hu-consent={"consent":true,"consentLevel":3,"consentID":"1cfd5dbc-1ab1-4368-a33a-51caddc06a93","categories":{"1":true,"2":true,"3":true,"4":true},"expiry":365,"timestamp":1732625470998,"blocking":{"blocked":0,"allowed":3,"blockedServices":[],"allowedServices":["2","34","58"]},"lastVersion":9}; _ga_0B23PQ385B=GS1.1.1732703510.2.1.1732703732.0.0.0; _ga=GA1.2.1143240425.1732625462',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        yield scrapy.Request('https://www.gtmstores.com/locations/',
                             cookies=cookies,
                             headers=headers,
                             callback=self.parse)

    def parse(self, response, **kwargs):
        selector = Selector(response.text)

        list_of_store = selector.xpath('//*[@data-id="9f8d33b"]/div')

        for store in list_of_store:
            item = GtmstoreItem()
            url = response.url
            store_no = 'N/A'

            try:
                name = store.xpath('.//h2[@class="elementor-heading-title elementor-size-default"]//text()').get()
            except Exception as e:
                name = 'N/A'

            details = store.xpath('.//div[@data-widget_type="text-editor.default"]//p')
            address = 'N/A'
            phone = 'N/A'
            poening_hrs = 'N/A'
            for position, ptag in enumerate(details):
                if position == 0 or position == 1:
                    address += ptag.xpath('.//text()').get().strip() + ', '
                if position == 2:
                    phone = ''.join(ptag.xpath('.//text()').getall()).strip()
                if position == 4 or position == 5 or position == 6:
                    poening_hrs += ptag.xpath('.//text()').get().strip() + ' '

            open_hours = remove_extra_space(convert_schedule(poening_hrs))

            street = remove_extra_space(address)[:-1]

            city, state_zip_code = street.split(',')[1:]
            city = remove_extra_space(city)
            state, zip_code = state_zip_code.strip().split(' ')

            latitude = 'N/A'
            longitude = 'N/A'

            county = 'N/A'


            provider = 'GTM Original'
            category = 'Apparel And Accessory Stores'

            updated_date = datetime.datetime.now().strftime("%d-%m-%Y")
            country = 'USA'
            status = 'Open'

            try:
                direction_url = store.xpath('.//a[contains(@href, "maps")]/@href').get()
            except Exception as e:
                direction_url = ''

            page_id = generate_hashid(response.url)
            pagesave_path = self.html_path + fr'{page_id}' + '.html.gz'
            gzip.open(pagesave_path, "wb").write(response.body)

            item['store_no'] = store_no
            item['name'] = name
            item['latitude'] = latitude
            item['longitude'] = longitude
            item['street'] = street
            item['city'] = city
            item['state'] = state
            item['zip_code'] = zip_code
            item['county'] = county
            item['phone'] = phone
            item['open_hours'] = open_hours
            item['url'] = url
            item['provider'] = provider
            item['category'] = category
            item['updated_date'] = updated_date
            item['country'] = country
            item['status'] = status
            item['direction_url'] = direction_url
            item['pagesave_path'] = pagesave_path
            yield item


if __name__ == '__main__':
    # execute("scrapy crawl kia".split())
    execute(f"scrapy crawl gtm -a start_id=0 -a end_id=100 -s CONCURRENT_REQUESTS=6".split())





