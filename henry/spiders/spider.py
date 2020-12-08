import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from time import time
from pprint import pprint
import os
import re
import json
import requests
from random import choice
from ORM import Operations
#from Mail import email
from datetime import datetime
import requests

class RootSpider(scrapy.Spider):
  name = "root"
  listings = []
  search_url = 'https://www.zillow.com/homes/TYPE/{}_rb/{}_p/'
  listing_url = 'https://www.zillow.com/homedetails/{}_zpid/'
  zip_search_results = "//script[@type='application/json']/text()"
  next_page_disabled = "//a[@rel='next']/@disabled"
  next_page_url_exists = "//a[@rel='next']/@href"
  recently_sold = "//*[@id='ds-container']/div[3]/div[2]/div/p/span[1]/text()"
  off_market = "//*[@id='ds-container']/div[3]/div[2]/div/p/span[1]/span[2]/span/text()"
  listing_errors = []
  zip_errors = []
  counters = { 'listings': 0, 'listing_errors': 0, 'zip_errors': 0 }
  start_time = datetime.now()
  properties = {}

  def write_listing_error(self, response, error, description):
    error = { 'error': error, 'description': description }

    zpid = response.url.split('_zpid')[0].split('/')[-1]
    if zpid.isdigit():
      error['zpid'] = int(zpid)

    self.listing_errors.append(error)

    self.counters['listing_errors'] += 1

  def write_zip_error(self, response, error, description):
    self.zip_errors.append({
      'zip': response.meta.get('zip'),
      'error': error,
      'description': description
    })

    self.counters['zip_errors'] += 1

  def quicksave(self):
    listings = []
    while len(self.listings) > 0:
      listings.append(self.listings.pop())

    if self.TYPE == 'for_sale':
      Operations.SaveListing(listings)
    elif self.TYPE == 'for_rent':
      Operations.SaveRentalListing(listings)


    listing_errors = []
    while len(self.listing_errors) > 0:
      listing_errors.append(self.listing_errors.pop())

    Operations.SaveListingError(listing_errors)

    zip_errors = []
    while len(self.zip_errors) > 0:
      zip_errors.append(self.zip_errors.pop()) 

    Operations.SaveZIPError(zip_errors)

  def load_proxies(self):
    self.proxies = ['http://{}:{}@{}:{}'.format(
      x.split(':')[2],
      x.split(':')[3],
      x.split(':')[0],
      x.split(':')[1]) for x in 
    requests.get(os.environ.get('PROXIES')).text.split('\r\n')[0:-2]]

  def get_proxy(self):
    if len(self.proxies) < 10:
      self.load_proxies()
    return choice(self.proxies)

  def start_requests(self):
    #self.TYPE = 'for_sale'
    self.search_url = self.search_url.replace('TYPE', self.TYPE)
    zips = [_zip.Value for _zip in Operations.QueryZIP()]
    self.load_proxies()

    for _zip in zips:
      yield scrapy.Request(url=self.search_url.format(_zip, 1),
        callback=self.parser,
        errback=self.errbacktest,
        meta={
          'proxy': self.get_proxy(), 
          'page': 1, 
          'zip': _zip})

  def parser(self, response):
    if 'captchaPerimeterX' in response.url:
      self.proxies.remove(response.meta.get('proxy'))
      self.write_zip_error(response, "Captcha", "Captcha")

    try:
      data = response.xpath(self.zip_search_results)[1].extract()
    except Exception as e:
      self.write_zip_error(response, str(e), "Error extracting search results application/json")
      return

    try:
      json_dict = json.loads(data.replace('<!--', '').replace('-->', ''))
    except Exception as e:
      self.write_zip_error(response, str(e), "Error loading json")
      return    

    try:
      urls = {x['detailUrl']: {"price": x['unformattedPrice'], "zpid": x['zpid']}
        for x in json_dict['cat1']['searchResults']['listResults']}

    except Exception as e:
      self.write_zip_error(response, str(e), "Could not find URL's")
      return

    # Get from DB existing listings in this zip code
    _zip = response.meta.get('zip')
    if response.meta.get('zip') not in self.properties:
      self.properties[_zip] = Operations.QueryZIPListings(_zip)

    # Properties
    for url, values in urls.items():

      if 'captchaPerimeterX' in url: 
        continue

      # start comparing with existing properties
      if values['zpid'] in self.properties[_zip]:
        saved_price = self.properties[_zip][values['zpid']]

        # if property is in properties we remove it
        # so we end up with self.properties[_zip] with only those that
        # are sold (possibly)
        self.properties[_zip].pop(values['zpid'])

        # if the saved_price matches the current price we dont open the url
        if values['price'] == saved_price:
          continue

      # Some urls are malformed
      if 'https://www.zillow.com' not in url:
        url = 'https://www.zillow.com' + url

      yield scrapy.Request(url=url,
        callback=self.listing,
        errback=self.errbacktest,
        meta={
          'proxy': self.get_proxy(),
          'zip': response.meta.get('zip')})

    # Next page
    if response.xpath(self.next_page_disabled).extract_first() == None:
      if response.xpath(self.next_page_url_exists).extract_first() == None: return

      yield scrapy.Request(self.search_url.format(response.meta.get('zip'), response.meta.get('page') + 1),
        callback=self.parser,
        errback=self.errbacktest,
        meta={
          'proxy': self.get_proxy(),
          'page': response.meta.get('page') + 1,
          'zip': response.meta.get('zip')})

    else:
      for zpid in self.properties[_zip].keys():
        yield scrapy.Request(url=self.listing_url.format(zpid),
          callback=self.listing,
          errback=self.errbacktest,
          meta={
            'proxy': self.get_proxy(), 
            'check_sold': True, 
            'zpid': zpid})


  def listing(self, response):
    # mark as off market those that are sold or off market
    if response.meta.get('check_sold', None) is not None:
      sold = response.xpath(self.recently_sold).extract()
      if len(sold) > 0 and sold[0] == 'Sold':
        Operations.ListingOffMarket(response.meta.get('zpid'))
        return

      off_market = response.xpath(self.off_market).extract()
      if len(off_market) > 0 and off_market[0] == 'Sold':
        Operations.ListingOffMarket(response.meta.get('zpid'))
        return

    if 'captchaPerimeterX' in response.url: 
      return

    if '_zpid' not in response.url :
      if response.meta.get('retry', None) is not None:
        yield scrapy.Request(url=url,
          callback=self.listing,
          errback=self.errbacktest,
          meta={
            'proxy': self.get_proxy(),
            'zip': response.meta.get('zip'),
            'retry': True})

      else: return

      return


    # Some ads are a list of units instead of a single property 
    if len(response.xpath("//a[@class='unit-card-link']")) > 0:
      print(response.url)
      for url in response.xpath("//a[@class='unit-card-link']/@href").extract():

        if 'captchaPerimeterX' in url: 
          continue

        if 'https://www.zillow.com' not in url:
          url = 'https://www.zillow.com' + url

        yield scrapy.Request(url=url,
          callback=self.listing,
          errback=self.errbacktest,
          meta={
            'proxy': self.get_proxy(), 
            'zip': response.meta.get('zip')})



    if len(self.listings) > 20:
      self.quicksave()

    try:
      json_data = response.xpath("//script[@type='application/json']/text()")[3].extract()
    except Exception as e:
      self.write_listing_error(response, str(e), "Error extracting listing application/json")
      return

    try:
      all_data = json.loads(json_data)
    except Exception as e:
      self.write_listing_error(response, str(e), "Error parsing listing json")
      return

    try:
      data = json.loads(all_data['apiCache'])
    except Exception as e:
      self.write_listing_error(response, str(e), "'apiCache' key problem")
      return

    try:
      keys = list(data.keys())
    except Exception as e:
      self.write_listing_error(response, str(e), "Could not extract keys")
      return

    try:
      info = data[keys[1]]['property']
    except Exception as e:
      self.write_listing_error(response, str(e), "'property' not found in data")
      return

    try:
      resofacts = info['resoFacts']
    except Exception as e:
      self.write_listing_error(response, str(e), "'resoFacts' not found in data")
      return

    result = info['resoFacts']

    result['dateSold'] = datetime.fromtimestamp(data[keys[0]]['property'].get('dateSold',0)/1000)
    try:
      result['datePosted'] = datetime.fromtimestamp(data[keys[1]]['property'].get('datePosted', 0)/1000)
    except Exception as e:
      result['datePosted'] = None

    print(info['zpid'])

    if not isinstance(info['zpid'], int): return

    result['brokerId'] = data[keys[0]]['property'].get('brokerId', None)
    result['price'] = data[keys[0]]['property'].get('price', 0)

    result['state'] = info['state']
    result['city'] = info['city']
    result['country'] = info['country']
    result['streetAddress'] = info['streetAddress']
    result['listing_sub_type'] = info['listing_sub_type']
    result['priceHistory'] = info.get('priceHistory', {})
    
    
    # Forgot to save
    result['longitude'] = info['longitude']
    result['latitude'] = info['latitude']
    result['lotSize'] = info['lotSize']
    result['brokerageName'] = info['brokerageName']
    result['livingAreaNumber'] = info['livingArea']

    result['zpid'] = info['zpid']
    result['zip'] = response.meta.get('zip')
    result['otherFacts'] = info['resoFacts']['otherFacts']
    result['atAGlanceFacts'] = info['resoFacts']['atAGlanceFacts']
    result['adTargets'] = info.get('adTargets', {})
    result['homeValues'] = info.get('homeValues', {})
    result['mortgageRates'] = info.get('mortgageRates', {})
    result['solarPotential'] = info.get('solarPotential', {})
    result['taxHistory'] = info.get('taxHistory', {})
    result['nearbyHomes'] = [x['zpid'] for x in info['nearbyHomes']]
    result['schools'] = info.get('schools', {})
    result['buildingPermits'] = info.get('buildingPermits', {})

    self.listings.append(result)
    self.counters['listings'] += 1

  def errbacktest(self, failiure):
    print(failiure)
    pass

  @classmethod
  def from_crawler(cls, crawler, *args, **kwargs):
    spider = super().from_crawler(crawler, *args, **kwargs)
    crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
    return spider

  def spider_closed(self, spider):
    self.quicksave()

    #email(os.environ.get('GMAIL'), os.environ.get('PASSWORD'), 
    #  os.environ.get('RECIPENT'), datetime.now(), self.counters['listings'],
    #  self.counters['listing_errors'], self.counters['zip_errors'])

    Operations.SaveLog({
      'counters': self.counters,
      'time': (datetime.now() - self.start_time).seconds
    })

if __name__ == "__main__":
  process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
    'TYPE': 'for_rent',
    'LOG_ENABLED': 1,
    'LOG_LEVEL': 'ERROR',
    'LOG_FORMAT': '%(levelname)s: %(message)s'
  })

  process.crawl(RootSpider)
  process.start()
