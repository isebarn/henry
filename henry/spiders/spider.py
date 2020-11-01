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
from Mail import email
from datetime import datetime
import requests

class RootSpider(scrapy.Spider):
  name = "root"
  listings = []
  search_url = 'https://www.zillow.com/homes/{}_rb/{}_p/'
  listing_url = 'https://www.zillow.com/homedetails/{}_zpid/'
  zip_search_results = "//script[@type='application/json']/text()"
  next_page_disabled = "//a[@rel='next']/@disabled"
  next_page_url_exists = "//a[@rel='next']/@href"
  listing_json_path = "//script[@type='application/json']/text()"
  listing_errors = []
  zip_errors = []
  counters = { 'listings': 0, 'listing_errors': 0, 'zip_errors': 0 }
  start_time = datetime.now()

  def write_listing_error(response, error, description):
    self.errors.append({
      'zpid': response.url.split('_zpid')[0].split('/')[-1],
      'error': error,
      'description': description
    })

    self.counters['listing_errors'] += 1

  def write_zip_error(response, error, description):
    self.errors.append({
      'zip': response.meta.get('zip'),
      'error': error,
      'description': description
    })

    self.counters['zip_errors'] += 1

  def quicksave(self):
    listings = []
    while len(self.listings) > 0:
      listings.append(self.listings.pop())

    Operations.SaveListing(listings)

    listing_errors = []
    while len(self.listing_errors) > 0:
      listing_errors.append(self.listing_errors.pop())

    Operations.SaveListingError(listing_errors)

    zip_errors = []
    while len(self.zip_errors) > 0:
      zip_errors.append(self.zip_errors.pop()) 

    Operations.SaveZIPError(zip_errors)


  def start_requests(self):
    zips = [_zip.Value for _zip in Operations.QueryZIP()]
    self.proxies = ['http://{}:{}@{}:{}'.format(
      x.split(':')[2],
      x.split(':')[3],
      x.split(':')[0],
      x.split(':')[1]) for x in 
    requests.get(os.environ.get('PROXIES')).text.split('\r\n')[0:-2]]

    for _zip in zips:
      yield scrapy.Request(url=self.search_url.format(_zip, 1),
        callback=self.parser,
        errback=self.errbacktest,
        meta={'proxy': choice(self.proxies), 'page': 1, 'zip': _zip})

  def parser(self, response):
    try:
      data = response.xpath(self.zip_search_results)[1].extract()
    except Exception as e:
      write_zip_error(response, str(e), "Error extracting search results application/json")
      return

    try:
      json_dict = json.loads(data.replace('<!--', '').replace('-->', ''))
    except Exception as e:
      write_zip_error(response, str(e), "Error loading json")
      return    

    try:
      urls = [x['detailUrl'] for x in json_dict['cat1']['searchResults']['listResults']]
    except Exception as e:
      write_zip_error(response, str(e), "Could not find URL's")
      return

    # Properties
    for url in urls:
      if 'captchaPerimeterX' in url: continue
      yield scrapy.Request(url=url,
        callback=self.listing,
        errback=self.errbacktest,
        meta={'proxy': choice(self.proxies), 'zip': response.meta.get('zip')})


    # Next page
    if response.xpath(self.next_page_disabled).extract_first() == None:
      if response.xpath(self.next_page_url_exists).extract_first() == None: return

      yield scrapy.Request(self.search_url.format(response.meta.get('zip'), response.meta.get('page') + 1),
        callback=self.parser,
        errback=self.errbacktest,
        meta={
          'proxy': choice(self.proxies),
          'page': response.meta.get('page') + 1,
          'zip': response.meta.get('zip')})


  def listing(self, response):
    if len(self.listings) > 20:
      self.quicksave()

    try:
      json_data = response.xpath(self.listing_json_path)[3].extract()
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

    data = info['resoFacts']
    data['zpid'] = info['zpid']
    data['zip'] = response.meta.get('zip')
    data['otherFacts'] = info['resoFacts']['otherFacts']
    data['atAGlanceFacts'] = info['resoFacts']['atAGlanceFacts']
    data['adTargets'] = info.get('adTargets', {})
    data['homeValues'] = info.get('homeValues', {})
    data['mortgageRates'] = info.get('mortgageRates', {})
    data['solarPotential'] = info.get('solarPotential', {})
    data['taxHistory'] = info.get('taxHistory', {})
    data['nearbyHomes'] = [x['zpid'] for x in info['nearbyHomes']]
    data['schools'] = info.get('schools', {})
    data['buildingPermits'] = info.get('buildingPermits', {})

    self.listings.append(data)
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
    Operations.SaveListing(self.listings)
    Operations.SaveListingError(self.listing_errors)
    Operations.SaveZIPError(self.zip_errors)

    email(os.environ.get('GMAIL'), os.environ.get('PASSWORD'), 
      os.environ.get('RECIPENT'), datetime.now(), self.counters['listings'],
      self.counters['listing_errors'], self.counters['zip_errors'])

    Operations.SaveLog({
      'counters': self.counters,
      'time': (datetime.now() - self.start_time).seconds
    })

if __name__ == "__main__":
  process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
    'LOG_ENABLED': 1,
    'LOG_LEVEL': 'ERROR',
    'LOG_FORMAT': '%(levelname)s: %(message)s'
  })

  process.crawl(RootSpider)
  process.start()
