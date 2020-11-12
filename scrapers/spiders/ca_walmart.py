from itertools import product
import scrapy
from scrapers.items import ProductItem
from scrapy.http.response.html import HtmlResponse
import json
import demjson

BRANCHES = [
    {'id': 3124, 'latitude': 48.4128269, 'longitude': -89.3097779},
    {'id': 3106, 'latitude': 43.6562242, 'longitude': -79.4355773},
]
STORE = 'Walmart'
API_URL = 'https://www.walmart.ca/api/product-page/find-in-store?latitude={}&longitude={}&lang=en&upc={}'

with open('cookies.json', 'r') as f:
    cookies = json.loads(f.read())


class CaWalmartSpider(scrapy.Spider):
    name = 'ca_walmart'
    allowed_domains = ['walmart.ca']

    def __get_data(self, response: HtmlResponse) -> dict:
        """Parse JavaScript into Python dict object"""
        js_obj = response.xpath(
            "//script[starts-with(text(),'window.__PRELOADED_STATE__')]/text()")
        js_obj = js_obj.get()[27:-1]
        return demjson.decode(js_obj)

    def __get_sku(self, data: dict) -> str:
        """Get product SKU"""
        return data['product']['activeSkuId']

    def __get_product_data(self, data: dict, sku: str) -> dict:
        """Get dictionary with product's data"""
        return data['entities']['skus'][sku]

    def __get_categories(self, data: dict, char: str = '|') -> str:
        """Returns categories as a string separated by pipe character"""
        categories_data = data['product']['item']['primaryCategories'][0]['hierarchy']
        return char.join([x['displayName']['en'] for x in categories_data])

    def __get_store(self) -> str:
        """Returns store name"""
        return STORE

    def __get_barcodes(self, product_data: dict, char=',') -> str:
        """Returns all barcodes as a string separated by commas"""
        return char.join(product_data['upc'])

    def __get_brand(self, product_data: dict) -> str:
        """Get product brand"""
        return product_data['brand']['name']

    def __get_description(self, product_data: dict) -> str:
        """Get product description"""
        return product_data['longDescription'].replace('<br>', '')

    def __get_name(self, product_data: dict) -> str:
        """Get product name"""
        return product_data['name']

    def __get_package(self, data: dict) -> str:
        """Get kind of packaging the product is sold in"""
        return data['product']['item']['description']

    def __get_image_url(self, product_data: dict) -> str:
        """Returns list of url images converted to string"""
        return str([x['large']['url']
                    for x in product_data['images']])

    def __get_item(self, response: HtmlResponse) -> ProductItem:
        """Get an instance of the class scrapy.Item that represents the scraped product"""
        data = self.__get_data(response)

        sku = self.__get_sku(data)
        product_data = self.__get_product_data(data, sku)

        product_item = ProductItem()
        product_item['sku'] = sku
        product_item['url'] = response.url
        product_item['store'] = self.__get_store()
        product_item['barcodes'] = self.__get_barcodes(product_data)
        product_item['brand'] = self.__get_brand(product_data)
        product_item['description'] = self.__get_description(product_data)
        product_item['name'] = self.__get_name(product_data)
        product_item['package'] = self.__get_package(data)
        product_item['image_url'] = self.__get_image_url(product_data)
        product_item['category'] = self.__get_categories(data)
        return product_item

    def start_requests(self):
        url = 'https://www.walmart.ca/en/grocery/fruits-vegetables/fruits/N-3852'
        yield scrapy.Request(url=url, cookies=cookies, callback=self.parse)

    def parse(self, response: HtmlResponse):
        products_links = response.xpath('//a[@class="product-link"]')
        yield from response.follow_all(products_links, callback=self.parse_product)

        next_page = response.css('a#loadmore::attr(href)').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    def parse_product(self, response: HtmlResponse):
        product_item = self.__get_item(response)

        for branch in BRANCHES:
            product_item['branch'] = branch['id']
            api_url = API_URL.format(
                branch["latitude"], branch["longitude"], product_item['barcodes'][0])
            yield scrapy.Request(url=api_url, cookies=cookies, callback=self.parse_api,
                                 cb_kwargs=dict(product_item=product_item.copy()))

    def parse_api(self, response: HtmlResponse, product_item: ProductItem):
        save = True
        stores_api_data = json.loads(response.body)['info']
        for store in stores_api_data:
            if store['id'] == product_item['branch']:
                try:
                    product_item['price'] = store['sellPrice']
                    product_item['stock'] = store['availableToSellQty']
                except KeyError:
                    save = False
                finally:
                    break
        if save:
            yield product_item
