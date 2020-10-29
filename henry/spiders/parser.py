import json
from pprint import pprint
'''
with open('/home/david/god.txt', 'r') as file:
  data = file.read().replace('\n', '')

a = json.loads(data)
a = json.loads(a['apiCache'])
keys = list(a.keys())
info = a[keys[1]]['property']
resofacts = info['resoFacts']

pprint(resofacts)
'''


#address
#bathrooms
#bedrooms
#buildingPermits
#comps (list?)
#homeFacts
#homeValues
#nearbyHomes
#priceHistory
#resoFacts
#restimateHighPercent
#restimateLowPercent
#schools
#solarPotential
#taxAssessedValue
#taxAssessedYear
#taxHistory
#timeOnZillow