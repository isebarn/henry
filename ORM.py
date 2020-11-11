import os
import json
from datetime import datetime
from sqlalchemy import ForeignKey, desc, create_engine, func, Column, BigInteger, JSON, Integer, Float, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker


engine = create_engine("postgresql://henry:henry123@192.168.1.35:5433/henry", echo=False)
Base = declarative_base()

def get_nullable_array(array_name, dictionary):
  if dictionary.get(array_name, []) != None:
    return dictionary.get(array_name, []) 
  else:
    return []

class Log(Base):
  __tablename__ = 'log'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', JSON)
  Date = Column('date', DateTime)
  Time = Column('time', Integer)

  def __init__(self, data):
    self.Value = data['counters']
    self.Time = data['time']
    self.Date = datetime.now()

class ZIP(Base):
  __tablename__ = 'zip'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String)

  def __init__(self, data):
    self.Id = data['id']
    self.Value = data['value']

class ListingError(Base):
  __tablename__ = 'listing_error'

  Id = Column('id', Integer, primary_key=True)
  ZPID = Column('zpid', Integer)
  Error = Column('error', String)
  Description = Column('description', String)

  def __init__(self, data):
    self.ZPID = data['zpid'] if data.get('zpid', None) is not None else 0
    self.Error = data['error']
    self.Description = data['description']

class ZIPError(Base):
  __tablename__ = 'zip_error'

  Id = Column('id', Integer, primary_key=True)
  ZIP = Column('zip', Integer)
  Error = Column('error', String)
  Description = Column('description', String)

  def __init__(self, data):
    self.ZIP = data['zip']
    self.Error = data['error']
    self.Description = data['description']

class Listing(Base):
  __tablename__ = 'listings'

  Id = Column('id', Integer, primary_key=True)
  DateSold = Column('dateSold', DateTime, nullable=True)
  OnMarketDate = Column('onMarketDate', DateTime, nullable=True)
  ScrapeDate = Column('scrapeDate', DateTime, nullable=True)
  DatePosted = Column('datePosted', DateTime, nullable=True)

  ArchitecturalStyle = Column('architecturalStyle', String, nullable=True)
  AssociationFee = Column('associationFee', String, nullable=True)
  AssociationFee2 = Column('associationFee2', String, nullable=True)
  AssociationName = Column('associationName', String, nullable=True)
  AssociationName2 = Column('associationName2', String, nullable=True)
  AssociationPhone = Column('associationPhone', String, nullable=True)
  AssociationPhone2 = Column('associationPhone2', String, nullable=True)
  Basement = Column('basement', String, nullable=True)
  BelowGradeFinishedArea = Column('belowGradeFinishedArea', String, nullable=True)
  BuilderModel = Column('builderModel', String, nullable=True)
  BuilderName = Column('builderName', String, nullable=True)
  BuildingAreaSource = Column('buildingAreaSource', String, nullable=True)
  BuildingName = Column('buildingName', String, nullable=True)
  City = Column('city', String, nullable=True)
  CityRegion = Column('cityRegion', String, nullable=True)
  CommonWalls = Column('commonWalls', String, nullable=True)
  ConstructionMaterials = Column('constructionMaterials', String, nullable=True)
  Country = Column('country', String, nullable=True)
  DevelopmentStatus = Column('developmentStatus', String, nullable=True)
  ElementarySchool = Column('elementarySchool', String, nullable=True)
  ElementarySchoolDistrict = Column('elementarySchoolDistrict', String, nullable=True)
  EntryLocation = Column('entryLocation', String, nullable=True)
  Fencing = Column('fencing', String, nullable=True)
  HighSchool = Column('highSchool', String, nullable=True)
  HighSchoolDistrict = Column('highSchoolDistrict', String, nullable=True)
  HomeType = Column('homeType', String, nullable=True)
  IsNewConstruction = Column('isNewConstruction', String, nullable=True)
  LandLeaseAmount = Column('landLeaseAmount', String, nullable=True)
  Levels = Column('levels', String, nullable=True)
  ListingId = Column('listingId', String, nullable=True)
  LivingArea = Column('livingArea', String, nullable=True)
  LotSize = Column('lotSize', String, nullable=True)
  MiddleOrJuniorSchool = Column('middleOrJuniorSchool', String, nullable=True)
  MiddleOrJuniorSchoolDistrict = Column('middleOrJuniorSchoolDistrict', String, nullable=True)
  OtherStructures = Column('otherStructures', String, nullable=True)
  ParcelNumber = Column('parcelNumber', String, nullable=True)
  PropertyCondition = Column('propertyCondition', String, nullable=True)
  RoofType = Column('roofType', String, nullable=True)
  State = Column('state', String, nullable=True)
  StreetAddress = Column('streetAddress', String, nullable=True)
  StructureType = Column('structureType', String, nullable=True)
  Topography = Column('topography', String, nullable=True)
  VirtualTour = Column('virtualTour', String, nullable=True)
  Zoning = Column('zoning', String, nullable=True)
  ZoningDescription = Column('zoningDescription', String, nullable=True)
  BrokerageName = Column('brokerageName', String, nullable=True)

  Bathrooms = Column('bathrooms', Integer, nullable=True)
  BathroomsFull = Column('bathroomsFull', Integer, nullable=True)
  BathroomsHalf = Column('bathroomsHalf', Integer, nullable=True)
  BathroomsOneQuarter = Column('bathroomsOneQuarter', Integer, nullable=True)
  BathroomsPartial = Column('bathroomsPartial', Integer, nullable=True)
  BathroomsThreeQuarter = Column('bathroomsThreeQuarter', Integer, nullable=True)
  Bedrooms = Column('bedrooms', Integer, nullable=True)
  BrokerId = Column('brokerId', Integer, nullable=True)
  CoveredSpaces = Column('coveredSpaces', Integer, nullable=True)
  Fireplaces = Column('fireplaces', Integer, nullable=True)
  GarageSpaces = Column('garageSpaces', Integer, nullable=True)
  NumberOfUnitsInCommunity = Column('numberOfUnitsInCommunity', Integer, nullable=True)
  OpenParkingSpaces = Column('openParkingSpaces', Integer, nullable=True)
  Parking = Column('parking', Integer, nullable=True)
  Price = Column('price', Integer, nullable=True)
  Stories = Column('stories', Integer, nullable=True)
  StoriesTotal = Column('storiesTotal', Integer, nullable=True)
  TaxAnnualAmount = Column('taxAnnualAmount', Integer, nullable=True)
  TaxAssessedValue = Column('taxAssessedValue', Integer, nullable=True)
  YearBuilt = Column('yearBuilt', Integer, nullable=True)
  ZIP = Column('zip', Integer, nullable=True)
  YearBuiltEffective = Column('yearBuiltEffective', Integer, nullable=True)
  LotSizeNumber = Column('lotSizeNumber', Integer, nullable=True)
  LivingAreaNumber = Column('livingAreaNumber', Integer, nullable=True)

  CanRaiseHorses = Column('canRaiseHorses', Boolean, nullable=True)
  Furnished = Column('furnished', Boolean, nullable=True)
  HasAdditionalParcels = Column('hasAdditionalParcels', Boolean, nullable=True)
  HasAssociation = Column('hasAssociation', Boolean, nullable=True)
  HasAttachedGarage = Column('hasAttachedGarage', Boolean, nullable=True)
  HasAttachedProperty = Column('hasAttachedProperty', Boolean, nullable=True)
  HasCarport = Column('hasCarport', Boolean, nullable=True)
  HasCooling = Column('hasCooling', Boolean, nullable=True)
  HasFireplace = Column('hasFireplace', Boolean, nullable=True)
  HasGarage = Column('hasGarage', Boolean, nullable=True)
  HasHeating = Column('hasHeating', Boolean, nullable=True)
  HasHomeWarranty = Column('hasHomeWarranty', Boolean, nullable=True)
  HasLandLease = Column('hasLandLease', Boolean, nullable=True)
  HasOpenParking = Column('hasOpenParking', Boolean, nullable=True)
  HasPetsAllowed = Column('hasPetsAllowed', Boolean, nullable=True)
  HasPrivatePool = Column('hasPrivatePool', Boolean, nullable=True)
  HasRentControl = Column('hasRentControl', Boolean, nullable=True)
  HasSpa = Column('hasSpa', Boolean, nullable=True)
  HasView = Column('hasView', Boolean, nullable=True)
  HasWaterfrontView = Column('hasWaterfrontView', Boolean, nullable=True)
  IsNewConstruction = Column('isNewConstruction', Boolean, nullable=True)
  AccessibilityFeatures = Column('accessibilityFeatures', ARRAY(String))
  Appliances = Column('appliances', JSON)
  AssociationAmenities = Column('associationAmenities', ARRAY(String))
  AssociationFeeIncludes = Column('associationFeeIncludes', ARRAY(String))
  AtAGlanceFacts = Column('atAGlanceFacts', ARRAY(String))
  BuildingFeatures = Column('buildingFeatures', ARRAY(String))
  CommunityFeatures = Column('communityFeatures', ARRAY(String))
  ConstructionMaterials = Column('constructionMaterials', ARRAY(String))
  Cooling = Column('cooling', ARRAY(String))
  Electric = Column('electric', ARRAY(String))
  ExteriorFeatures = Column('exteriorFeatures', ARRAY(String))
  FireplaceFeatures = Column('fireplaceFeatures', ARRAY(String))
  Flooring = Column('flooring', ARRAY(String))
  FoundationDetails = Column('foundationDetails', ARRAY(String))
  Gas = Column('gas', ARRAY(String))
  GreenBuildingVerificationType = Column('greenBuildingVerificationType', ARRAY(String))
  GreenEnergyEfficient = Column('greenEnergyEfficient', ARRAY(String))
  GreenWaterConservation = Column('greenWaterConservation', ARRAY(String))
  Heating = Column('heating', ARRAY(String))
  LaundryFeatures = Column('laundryFeatures', ARRAY(String))
  OtherFacts = Column('otherFacts', ARRAY(String))
  OtherStructures = Column('otherStructures', ARRAY(String))
  ParkingFeatures = Column('parkingFeatures', ARRAY(String))
  PatioAndPorchFeatures = Column('patioAndPorchFeatures', ARRAY(String))
  Rooms = Column('rooms', ARRAY(String))
  SecurityFeatures = Column('securityFeatures', ARRAY(String))
  Sewer = Column('sewer', ARRAY(String))
  SpaFeatures = Column('spaFeatures', ARRAY(String))
  Utilities = Column('utilities', ARRAY(String))
  Vegetation = Column('vegetation', ARRAY(String))
  View = Column('view', ARRAY(String))
  WaterfrontFeatures = Column('waterfrontFeatures', ARRAY(String))
  WindowFeatures = Column('windowFeatures', ARRAY(String))
  AtAGlanceFacts = Column('atAGlanceFacts', JSON)
  AdTargets = Column('adTargets', JSON)
  HomeValues = Column('homeValues', JSON)
  MortgageRates = Column('mortgageRates', JSON)
  SolarPotential = Column('solarPotential', JSON)
  TaxHistory = Column('taxHistory', JSON)
  NearbyHomes = Column('nearbyHomes', JSON)
  Schools = Column('schools', JSON)
  BuildingPermits = Column('buildingPermits', JSON)
  ListingSubType = Column('listing_sub_type', JSON)
  PriceHistory = Column('priceHistory', JSON)
  AboveGradeFinishedArea = Column('aboveGradeFinishedArea', String)
  AdditionalParcelsDescription = Column('additionalParcelsDescription', String)
  BuildingArea = Column('buildingArea', String)
  CarportSpaces = Column('carportSpaces', String)
  EntryLevel = Column('entryLevel', String)
  FrontageLength = Column('frontageLength', String)
  FrontageType = Column('frontageType', String)
  GreenIndoorAirQuality = Column('greenIndoorAirQuality', String)
  GreenSustainability = Column('greenSustainability', String)
  HasElectricOnProperty = Column('hasElectricOnProperty', String)
  IsSeniorCommunity = Column('isSeniorCommunity', String)
  LotSizeDimensions = Column('lotSizeDimensions', String)
  MainLevelBathrooms = Column('mainLevelBathrooms', String)
  NumberOfUnitsVacant = Column('numberOfUnitsVacant', String)
  OtherParking = Column('otherParking', String)
  WaterSources = Column('waterSources', String)
  WoodedArea = Column('woodedArea', String)

  Longitude = Column('longitude', Float)
  Latitude = Column('latitude', Float)


  def __init__(self, data):
    # Id
    self.Id = data['zpid']

    # Dates
    if data.get('onMarketDate', None) != None:
      self.OnMarketDate = datetime.fromtimestamp(data['onMarketDate']/1000)
    else:
      self.OnMarketDate = None

    self.DateSold = data.get('dateSold', None)
    self.DatePosted = data.get('datePosted', None)

    self.ScrapeDate = datetime.now()
    self.ZIP = data['zip']

    # Strings
    self.ArchitecturalStyle = data.get('architecturalStyle', None)
    self.AssociationFee = data.get('associationFee', None)
    self.AssociationFee2 = data.get('associationFee2', None)
    self.AssociationName = data.get('associationName', None)
    self.AssociationName2 = data.get('associationName2', None)
    self.AssociationPhone = data.get('associationPhone', None)
    self.AssociationPhone2 = data.get('associationPhone2', None)
    self.Basement = data.get('basement', None)
    self.BelowGradeFinishedArea = data.get('belowGradeFinishedArea', None)
    self.BuilderModel = data.get('builderModel', None)
    self.BuilderName = data.get('builderName', None)
    self.BuildingAreaSource = data.get('buildingAreaSource', None)
    self.BuildingName = data.get('buildingName', None)
    self.City = data.get('city', None)
    self.CityRegion = data.get('cityRegion', None)
    self.CommonWalls = data.get('commonWalls', None)
    self.ConstructionMaterials = data.get('constructionMaterials', None)
    self.Country = data.get('country', None)
    self.DevelopmentStatus = data.get('developmentStatus', None)
    self.ElementarySchool = data.get('elementarySchool', None)
    self.ElementarySchoolDistrict = data.get('elementarySchoolDistrict', None)
    self.EntryLocation = data.get('entryLocation', None)
    self.Fencing = data.get('fencing', None)
    self.HighSchool = data.get('highSchool', None)
    self.HighSchoolDistrict = data.get('highSchoolDistrict', None)
    self.HomeType = data.get('homeType', None)
    self.IsNewConstruction = data.get('isNewConstruction', None)
    self.LandLeaseAmount = data.get('landLeaseAmount', None)
    self.Levels = data.get('levels', None)
    self.ListingId = data.get('listingId', None)
    self.LivingArea = data.get('livingArea', None)
    self.LotSize = data.get('lotSize', None)
    self.MiddleOrJuniorSchool = data.get('middleOrJuniorSchool', None)
    self.MiddleOrJuniorSchoolDistrict = data.get('middleOrJuniorSchoolDistrict', None)
    self.OtherStructures = data.get('otherStructures', None)
    self.ParcelNumber = data.get('parcelNumber', None)
    self.PropertyCondition = data.get('propertyCondition', None)
    self.RoofType = data.get('roofType', None)
    self.State = data.get('state', None)
    self.StreetAddress = data.get('streetAddress', None)
    self.StructureType = data.get('structureType', None)
    self.Topography = data.get('topography', None)
    self.VirtualTour = data.get('virtualTour', None)
    self.Zoning = data.get('zoning', None)
    self.ZoningDescription = data.get('zoningDescription', None)
    self.BrokerageName = data.get('brokerageName', None)

    # Integers
    self.Bathrooms = data.get('bathrooms', None)
    self.BathroomsFull = data.get('bathroomsFull', None)
    self.BathroomsHalf = data.get('bathroomsHalf', None)
    self.BathroomsOneQuarter = data.get('bathroomsOneQuarter', None)
    self.BathroomsPartial = data.get('bathroomsPartial', None)
    self.BathroomsThreeQuarter = data.get('bathroomsThreeQuarter', None)
    self.Bedrooms = data.get('bedrooms', None)
    self.BrokerId = data.get('brokerId', None)
    self.CoveredSpaces = data.get('coveredSpaces', None)
    self.Fireplaces = data.get('fireplaces', None)
    self.LotSizeNumber = data.get('lotSize', None)
    self.GarageSpaces = data.get('garageSpaces', None)
    self.NumberOfUnitsInCommunity = data.get('numberOfUnitsInCommunity', None)
    self.OpenParkingSpaces = data.get('openParkingSpaces', None)
    self.Parking = data.get('parking', None)
    self.Price = data.get('price', None)
    self.Stories = data.get('stories', None)
    self.StoriesTotal = data.get('storiesTotal', None)
    self.TaxAnnualAmount = data.get('taxAnnualAmount', None)
    self.TaxAssessedValue = data.get('taxAssessedValue', None)
    self.YearBuilt = data.get('yearBuilt', None)
    self.YearBuiltEffective = data.get('yearBuiltEffective', None)
    self.LivingAreaNumber = data.get('livingAreaNumber', None)

    # Floats
    self.Longitude = data.get('longitude', None)
    self.Latitude = data.get('latitude', None)

    # Booleans
    self.CanRaiseHorses = data.get('canRaiseHorses', None)
    self.Furnished = data.get('furnished', None)
    self.HasAdditionalParcels = data.get('hasAdditionalParcels', None)
    self.HasAssociation = data.get('hasAssociation', None)
    self.HasAttachedGarage = data.get('hasAttachedGarage', None)
    self.HasAttachedProperty = data.get('hasAttachedProperty', None)
    self.HasCarport = data.get('hasCarport', None)
    self.HasCooling = data.get('hasCooling', None)
    self.HasFireplace = data.get('hasFireplace', None)
    self.HasGarage = data.get('hasGarage', None)
    self.HasHeating = data.get('hasHeating', None)
    self.HasHomeWarranty = data.get('hasHomeWarranty', None)
    self.HasLandLease = data.get('hasLandLease', None)
    self.HasOpenParking = data.get('hasOpenParking', None)
    self.HasPetsAllowed = data.get('hasPetsAllowed', None)
    self.HasPrivatePool = data.get('hasPrivatePool', None)
    self.HasRentControl = data.get('hasRentControl', None)
    self.HasSpa = data.get('hasSpa', None)
    self.HasView = data.get('hasView', None)
    self.HasWaterfrontView = data.get('hasWaterfrontView', None)
    self.IsNewConstruction = data.get('isNewConstruction', None)

    # Array's - Types not been determined
    self.AccessibilityFeatures = [str(x) for x in get_nullable_array('accessibilityFeatures', data)]
    self.Appliances = [str(x) for x in get_nullable_array('appliances', data)]
    self.AssociationAmenities = [str(x) for x in get_nullable_array('associationAmenities', data)]
    self.AssociationFeeIncludes = [str(x) for x in get_nullable_array('associationFeeIncludes', data)]
    self.AtAGlanceFacts = [str(x) for x in get_nullable_array('atAGlanceFacts', data)]
    self.BuildingFeatures = [str(x) for x in get_nullable_array('buildingFeatures', data)]
    self.CommunityFeatures = [str(x) for x in get_nullable_array('communityFeatures', data)]
    self.ConstructionMaterials = [str(x) for x in get_nullable_array('constructionMaterials', data)]
    self.Cooling = [str(x) for x in get_nullable_array('cooling', data)]
    self.Electric = [str(x) for x in get_nullable_array('electric', data)]
    self.ExteriorFeatures = [str(x) for x in get_nullable_array('exteriorFeatures', data)]
    self.FireplaceFeatures = [str(x) for x in get_nullable_array('fireplaceFeatures', data)]
    self.Flooring = [str(x) for x in get_nullable_array('flooring', data)]
    self.FoundationDetails = [str(x) for x in get_nullable_array('foundationDetails', data)]
    self.Gas = [str(x) for x in get_nullable_array('gas', data)]
    self.GreenBuildingVerificationType = [str(x) for x in get_nullable_array('greenBuildingVerificationType', data)]
    self.GreenEnergyEfficient = [str(x) for x in get_nullable_array('greenEnergyEfficient', data)]
    self.GreenWaterConservation = [str(x) for x in get_nullable_array('greenWaterConservation', data)]
    self.Heating = [str(x) for x in get_nullable_array('heating', data)]
    self.LaundryFeatures = [str(x) for x in get_nullable_array('laundryFeatures', data)]
    self.OtherFacts = [str(x) for x in get_nullable_array('otherFacts', data)]
    self.OtherStructures = [str(x) for x in get_nullable_array('otherStructures', data)]
    self.ParkingFeatures = [str(x) for x in get_nullable_array('parkingFeatures', data)]
    self.PatioAndPorchFeatures = [str(x) for x in get_nullable_array('patioAndPorchFeatures', data)]
    self.Rooms = [str(x) for x in get_nullable_array('rooms', data)]
    self.SecurityFeatures = [str(x) for x in get_nullable_array('securityFeatures', data)]
    self.Sewer = [str(x) for x in get_nullable_array('sewer', data)]
    self.SpaFeatures = [str(x) for x in get_nullable_array('spaFeatures', data)]
    self.Utilities = [str(x) for x in get_nullable_array('utilities', data)]
    self.Vegetation = [str(x) for x in get_nullable_array('vegetation', data)]
    self.View = [str(x) for x in get_nullable_array('view', data)]
    self.WaterfrontFeatures = [str(x) for x in get_nullable_array('waterfrontFeatures', data)]
    self.WindowFeatures = [str(x) for x in get_nullable_array('windowFeatures', data)]

    #Dicts
    self.AtAGlanceFacts = data.get('atAGlanceFacts', {})
    self.AdTargets = data.get('adTargets', {})
    self.HomeValues = data.get('homeValues', {})
    self.MortgageRates = data.get('mortgageRates', {})
    self.SolarPotential = data.get('solarPotential', {})
    self.TaxHistory = data.get('taxHistory', {})
    self.NearbyHomes = data.get('nearbyHomes', {})
    self.Schools = data.get('schools', {})
    self.BuildingPermits = data.get('buildingPermits', {})
    self.ListingSubType = data.get('listing_sub_type', {})
    self.PriceHistory = data.get('priceHistory', {})

    # Unknown - Never seen IRL
    self.AboveGradeFinishedArea = str(data['aboveGradeFinishedArea'])
    self.AdditionalParcelsDescription = str(data['additionalParcelsDescription'])
    self.BuildingArea = str(data['buildingArea'])
    self.CarportSpaces = str(data['carportSpaces'])
    self.EntryLevel = str(data['entryLevel'])
    self.FrontageLength = str(data['frontageLength'])
    self.FrontageType = str(data['frontageType'])
    self.GreenIndoorAirQuality = str(data['greenIndoorAirQuality'])
    self.GreenSustainability = str(data['greenSustainability'])
    self.HasElectricOnProperty = str(data['hasElectricOnProperty'])
    self.IsSeniorCommunity = str(data['isSeniorCommunity'])
    self.LotSizeDimensions = str(data['lotSizeDimensions'])
    self.MainLevelBathrooms = str(data['mainLevelBathrooms'])
    self.NumberOfUnitsVacant = str(data['numberOfUnitsVacant'])
    self.OtherParking = str(data['otherParking'])
    self.WaterSources = str(data['waterSources'])
    self.WoodedArea = str(data['woodedArea'])

class RentalListing(Base):
  __tablename__ = 'rentals'

  Id = Column('id', Integer, primary_key=True)
  DateSold = Column('dateSold', DateTime, nullable=True)
  OnMarketDate = Column('onMarketDate', DateTime, nullable=True)
  ScrapeDate = Column('scrapeDate', DateTime, nullable=True)
  DatePosted = Column('datePosted', DateTime, nullable=True)

  ArchitecturalStyle = Column('architecturalStyle', String, nullable=True)
  AssociationFee = Column('associationFee', String, nullable=True)
  AssociationFee2 = Column('associationFee2', String, nullable=True)
  AssociationName = Column('associationName', String, nullable=True)
  AssociationName2 = Column('associationName2', String, nullable=True)
  AssociationPhone = Column('associationPhone', String, nullable=True)
  AssociationPhone2 = Column('associationPhone2', String, nullable=True)
  Basement = Column('basement', String, nullable=True)
  BelowGradeFinishedArea = Column('belowGradeFinishedArea', String, nullable=True)
  BuilderModel = Column('builderModel', String, nullable=True)
  BuilderName = Column('builderName', String, nullable=True)
  BuildingAreaSource = Column('buildingAreaSource', String, nullable=True)
  BuildingName = Column('buildingName', String, nullable=True)
  City = Column('city', String, nullable=True)
  CityRegion = Column('cityRegion', String, nullable=True)
  CommonWalls = Column('commonWalls', String, nullable=True)
  ConstructionMaterials = Column('constructionMaterials', String, nullable=True)
  Country = Column('country', String, nullable=True)
  DevelopmentStatus = Column('developmentStatus', String, nullable=True)
  ElementarySchool = Column('elementarySchool', String, nullable=True)
  ElementarySchoolDistrict = Column('elementarySchoolDistrict', String, nullable=True)
  EntryLocation = Column('entryLocation', String, nullable=True)
  Fencing = Column('fencing', String, nullable=True)
  HighSchool = Column('highSchool', String, nullable=True)
  HighSchoolDistrict = Column('highSchoolDistrict', String, nullable=True)
  HomeType = Column('homeType', String, nullable=True)
  IsNewConstruction = Column('isNewConstruction', String, nullable=True)
  LandLeaseAmount = Column('landLeaseAmount', String, nullable=True)
  Levels = Column('levels', String, nullable=True)
  ListingId = Column('listingId', String, nullable=True)
  LivingArea = Column('livingArea', String, nullable=True)
  LotSize = Column('lotSize', String, nullable=True)
  MiddleOrJuniorSchool = Column('middleOrJuniorSchool', String, nullable=True)
  MiddleOrJuniorSchoolDistrict = Column('middleOrJuniorSchoolDistrict', String, nullable=True)
  OtherStructures = Column('otherStructures', String, nullable=True)
  ParcelNumber = Column('parcelNumber', String, nullable=True)
  PropertyCondition = Column('propertyCondition', String, nullable=True)
  RoofType = Column('roofType', String, nullable=True)
  State = Column('state', String, nullable=True)
  StreetAddress = Column('streetAddress', String, nullable=True)
  StructureType = Column('structureType', String, nullable=True)
  Topography = Column('topography', String, nullable=True)
  VirtualTour = Column('virtualTour', String, nullable=True)
  Zoning = Column('zoning', String, nullable=True)
  ZoningDescription = Column('zoningDescription', String, nullable=True)
  BrokerageName = Column('brokerageName', String, nullable=True)

  Bathrooms = Column('bathrooms', Integer, nullable=True)
  BathroomsFull = Column('bathroomsFull', Integer, nullable=True)
  BathroomsHalf = Column('bathroomsHalf', Integer, nullable=True)
  BathroomsOneQuarter = Column('bathroomsOneQuarter', Integer, nullable=True)
  BathroomsPartial = Column('bathroomsPartial', Integer, nullable=True)
  BathroomsThreeQuarter = Column('bathroomsThreeQuarter', Integer, nullable=True)
  Bedrooms = Column('bedrooms', Integer, nullable=True)
  BrokerId = Column('brokerId', Integer, nullable=True)
  CoveredSpaces = Column('coveredSpaces', Integer, nullable=True)
  Fireplaces = Column('fireplaces', Integer, nullable=True)
  GarageSpaces = Column('garageSpaces', Integer, nullable=True)
  NumberOfUnitsInCommunity = Column('numberOfUnitsInCommunity', Integer, nullable=True)
  OpenParkingSpaces = Column('openParkingSpaces', Integer, nullable=True)
  Parking = Column('parking', Integer, nullable=True)
  Price = Column('price', Integer, nullable=True)
  Stories = Column('stories', Integer, nullable=True)
  StoriesTotal = Column('storiesTotal', Integer, nullable=True)
  TaxAnnualAmount = Column('taxAnnualAmount', Integer, nullable=True)
  TaxAssessedValue = Column('taxAssessedValue', Integer, nullable=True)
  YearBuilt = Column('yearBuilt', Integer, nullable=True)
  ZIP = Column('zip', Integer, nullable=True)
  YearBuiltEffective = Column('yearBuiltEffective', Integer, nullable=True)
  LotSizeNumber = Column('lotSizeNumber', Integer, nullable=True)
  LivingAreaNumber = Column('livingAreaNumber', Integer, nullable=True)

  CanRaiseHorses = Column('canRaiseHorses', Boolean, nullable=True)
  Furnished = Column('furnished', Boolean, nullable=True)
  HasAdditionalParcels = Column('hasAdditionalParcels', Boolean, nullable=True)
  HasAssociation = Column('hasAssociation', Boolean, nullable=True)
  HasAttachedGarage = Column('hasAttachedGarage', Boolean, nullable=True)
  HasAttachedProperty = Column('hasAttachedProperty', Boolean, nullable=True)
  HasCarport = Column('hasCarport', Boolean, nullable=True)
  HasCooling = Column('hasCooling', Boolean, nullable=True)
  HasFireplace = Column('hasFireplace', Boolean, nullable=True)
  HasGarage = Column('hasGarage', Boolean, nullable=True)
  HasHeating = Column('hasHeating', Boolean, nullable=True)
  HasHomeWarranty = Column('hasHomeWarranty', Boolean, nullable=True)
  HasLandLease = Column('hasLandLease', Boolean, nullable=True)
  HasOpenParking = Column('hasOpenParking', Boolean, nullable=True)
  HasPetsAllowed = Column('hasPetsAllowed', Boolean, nullable=True)
  HasPrivatePool = Column('hasPrivatePool', Boolean, nullable=True)
  HasRentControl = Column('hasRentControl', Boolean, nullable=True)
  HasSpa = Column('hasSpa', Boolean, nullable=True)
  HasView = Column('hasView', Boolean, nullable=True)
  HasWaterfrontView = Column('hasWaterfrontView', Boolean, nullable=True)
  IsNewConstruction = Column('isNewConstruction', Boolean, nullable=True)
  AccessibilityFeatures = Column('accessibilityFeatures', ARRAY(String))
  Appliances = Column('appliances', JSON)
  AssociationAmenities = Column('associationAmenities', ARRAY(String))
  AssociationFeeIncludes = Column('associationFeeIncludes', ARRAY(String))
  AtAGlanceFacts = Column('atAGlanceFacts', ARRAY(String))
  BuildingFeatures = Column('buildingFeatures', ARRAY(String))
  CommunityFeatures = Column('communityFeatures', ARRAY(String))
  ConstructionMaterials = Column('constructionMaterials', ARRAY(String))
  Cooling = Column('cooling', ARRAY(String))
  Electric = Column('electric', ARRAY(String))
  ExteriorFeatures = Column('exteriorFeatures', ARRAY(String))
  FireplaceFeatures = Column('fireplaceFeatures', ARRAY(String))
  Flooring = Column('flooring', ARRAY(String))
  FoundationDetails = Column('foundationDetails', ARRAY(String))
  Gas = Column('gas', ARRAY(String))
  GreenBuildingVerificationType = Column('greenBuildingVerificationType', ARRAY(String))
  GreenEnergyEfficient = Column('greenEnergyEfficient', ARRAY(String))
  GreenWaterConservation = Column('greenWaterConservation', ARRAY(String))
  Heating = Column('heating', ARRAY(String))
  LaundryFeatures = Column('laundryFeatures', ARRAY(String))
  OtherFacts = Column('otherFacts', ARRAY(String))
  OtherStructures = Column('otherStructures', ARRAY(String))
  ParkingFeatures = Column('parkingFeatures', ARRAY(String))
  PatioAndPorchFeatures = Column('patioAndPorchFeatures', ARRAY(String))
  Rooms = Column('rooms', ARRAY(String))
  SecurityFeatures = Column('securityFeatures', ARRAY(String))
  Sewer = Column('sewer', ARRAY(String))
  SpaFeatures = Column('spaFeatures', ARRAY(String))
  Utilities = Column('utilities', ARRAY(String))
  Vegetation = Column('vegetation', ARRAY(String))
  View = Column('view', ARRAY(String))
  WaterfrontFeatures = Column('waterfrontFeatures', ARRAY(String))
  WindowFeatures = Column('windowFeatures', ARRAY(String))
  AtAGlanceFacts = Column('atAGlanceFacts', JSON)
  AdTargets = Column('adTargets', JSON)
  HomeValues = Column('homeValues', JSON)
  MortgageRates = Column('mortgageRates', JSON)
  SolarPotential = Column('solarPotential', JSON)
  TaxHistory = Column('taxHistory', JSON)
  NearbyHomes = Column('nearbyHomes', JSON)
  Schools = Column('schools', JSON)
  BuildingPermits = Column('buildingPermits', JSON)
  ListingSubType = Column('listing_sub_type', JSON)
  PriceHistory = Column('priceHistory', JSON)
  AboveGradeFinishedArea = Column('aboveGradeFinishedArea', String)
  AdditionalParcelsDescription = Column('additionalParcelsDescription', String)
  BuildingArea = Column('buildingArea', String)
  CarportSpaces = Column('carportSpaces', String)
  EntryLevel = Column('entryLevel', String)
  FrontageLength = Column('frontageLength', String)
  FrontageType = Column('frontageType', String)
  GreenIndoorAirQuality = Column('greenIndoorAirQuality', String)
  GreenSustainability = Column('greenSustainability', String)
  HasElectricOnProperty = Column('hasElectricOnProperty', String)
  IsSeniorCommunity = Column('isSeniorCommunity', String)
  LotSizeDimensions = Column('lotSizeDimensions', String)
  MainLevelBathrooms = Column('mainLevelBathrooms', String)
  NumberOfUnitsVacant = Column('numberOfUnitsVacant', String)
  OtherParking = Column('otherParking', String)
  WaterSources = Column('waterSources', String)
  WoodedArea = Column('woodedArea', String)

  Longitude = Column('longitude', Float)
  Latitude = Column('latitude', Float)


  def __init__(self, data):
    # Id
    self.Id = data['zpid']

    # Dates
    if data.get('onMarketDate', None) != None:
      self.OnMarketDate = datetime.fromtimestamp(data['onMarketDate']/1000)
    else:
      self.OnMarketDate = None

    self.DateSold = data.get('dateSold', None)
    self.DatePosted = data.get('datePosted', None)

    self.ScrapeDate = datetime.now()
    self.ZIP = data['zip']

    # Strings
    self.ArchitecturalStyle = data.get('architecturalStyle', None)
    self.AssociationFee = data.get('associationFee', None)
    self.AssociationFee2 = data.get('associationFee2', None)
    self.AssociationName = data.get('associationName', None)
    self.AssociationName2 = data.get('associationName2', None)
    self.AssociationPhone = data.get('associationPhone', None)
    self.AssociationPhone2 = data.get('associationPhone2', None)
    self.Basement = data.get('basement', None)
    self.BelowGradeFinishedArea = data.get('belowGradeFinishedArea', None)
    self.BuilderModel = data.get('builderModel', None)
    self.BuilderName = data.get('builderName', None)
    self.BuildingAreaSource = data.get('buildingAreaSource', None)
    self.BuildingName = data.get('buildingName', None)
    self.City = data.get('city', None)
    self.CityRegion = data.get('cityRegion', None)
    self.CommonWalls = data.get('commonWalls', None)
    self.ConstructionMaterials = data.get('constructionMaterials', None)
    self.Country = data.get('country', None)
    self.DevelopmentStatus = data.get('developmentStatus', None)
    self.ElementarySchool = data.get('elementarySchool', None)
    self.ElementarySchoolDistrict = data.get('elementarySchoolDistrict', None)
    self.EntryLocation = data.get('entryLocation', None)
    self.Fencing = data.get('fencing', None)
    self.HighSchool = data.get('highSchool', None)
    self.HighSchoolDistrict = data.get('highSchoolDistrict', None)
    self.HomeType = data.get('homeType', None)
    self.IsNewConstruction = data.get('isNewConstruction', None)
    self.LandLeaseAmount = data.get('landLeaseAmount', None)
    self.Levels = data.get('levels', None)
    self.ListingId = data.get('listingId', None)
    self.LivingArea = data.get('livingArea', None)
    self.LotSize = data.get('lotSize', None)
    self.MiddleOrJuniorSchool = data.get('middleOrJuniorSchool', None)
    self.MiddleOrJuniorSchoolDistrict = data.get('middleOrJuniorSchoolDistrict', None)
    self.OtherStructures = data.get('otherStructures', None)
    self.ParcelNumber = data.get('parcelNumber', None)
    self.PropertyCondition = data.get('propertyCondition', None)
    self.RoofType = data.get('roofType', None)
    self.State = data.get('state', None)
    self.StreetAddress = data.get('streetAddress', None)
    self.StructureType = data.get('structureType', None)
    self.Topography = data.get('topography', None)
    self.VirtualTour = data.get('virtualTour', None)
    self.Zoning = data.get('zoning', None)
    self.ZoningDescription = data.get('zoningDescription', None)
    self.BrokerageName = data.get('brokerageName', None)

    # Integers
    self.Bathrooms = data.get('bathrooms', None)
    self.BathroomsFull = data.get('bathroomsFull', None)
    self.BathroomsHalf = data.get('bathroomsHalf', None)
    self.BathroomsOneQuarter = data.get('bathroomsOneQuarter', None)
    self.BathroomsPartial = data.get('bathroomsPartial', None)
    self.BathroomsThreeQuarter = data.get('bathroomsThreeQuarter', None)
    self.Bedrooms = data.get('bedrooms', None)
    self.BrokerId = data.get('brokerId', None)
    self.CoveredSpaces = data.get('coveredSpaces', None)
    self.Fireplaces = data.get('fireplaces', None)
    self.LotSizeNumber = data.get('lotSize', None)
    self.GarageSpaces = data.get('garageSpaces', None)
    self.NumberOfUnitsInCommunity = data.get('numberOfUnitsInCommunity', None)
    self.OpenParkingSpaces = data.get('openParkingSpaces', None)
    self.Parking = data.get('parking', None)
    self.Price = data.get('price', None)
    self.Stories = data.get('stories', None)
    self.StoriesTotal = data.get('storiesTotal', None)
    self.TaxAnnualAmount = data.get('taxAnnualAmount', None)
    self.TaxAssessedValue = data.get('taxAssessedValue', None)
    self.YearBuilt = data.get('yearBuilt', None)
    self.YearBuiltEffective = data.get('yearBuiltEffective', None)
    self.LivingAreaNumber = data.get('livingAreaNumber', None)

    # Floats
    self.Longitude = data.get('longitude', None)
    self.Latitude = data.get('latitude', None)

    # Booleans
    self.CanRaiseHorses = data.get('canRaiseHorses', None)
    self.Furnished = data.get('furnished', None)
    self.HasAdditionalParcels = data.get('hasAdditionalParcels', None)
    self.HasAssociation = data.get('hasAssociation', None)
    self.HasAttachedGarage = data.get('hasAttachedGarage', None)
    self.HasAttachedProperty = data.get('hasAttachedProperty', None)
    self.HasCarport = data.get('hasCarport', None)
    self.HasCooling = data.get('hasCooling', None)
    self.HasFireplace = data.get('hasFireplace', None)
    self.HasGarage = data.get('hasGarage', None)
    self.HasHeating = data.get('hasHeating', None)
    self.HasHomeWarranty = data.get('hasHomeWarranty', None)
    self.HasLandLease = data.get('hasLandLease', None)
    self.HasOpenParking = data.get('hasOpenParking', None)
    self.HasPetsAllowed = data.get('hasPetsAllowed', None)
    self.HasPrivatePool = data.get('hasPrivatePool', None)
    self.HasRentControl = data.get('hasRentControl', None)
    self.HasSpa = data.get('hasSpa', None)
    self.HasView = data.get('hasView', None)
    self.HasWaterfrontView = data.get('hasWaterfrontView', None)
    self.IsNewConstruction = data.get('isNewConstruction', None)

    # Array's - Types not been determined
    self.AccessibilityFeatures = [str(x) for x in get_nullable_array('accessibilityFeatures', data)]
    self.Appliances = [str(x) for x in get_nullable_array('appliances', data)]
    self.AssociationAmenities = [str(x) for x in get_nullable_array('associationAmenities', data)]
    self.AssociationFeeIncludes = [str(x) for x in get_nullable_array('associationFeeIncludes', data)]
    self.AtAGlanceFacts = [str(x) for x in get_nullable_array('atAGlanceFacts', data)]
    self.BuildingFeatures = [str(x) for x in get_nullable_array('buildingFeatures', data)]
    self.CommunityFeatures = [str(x) for x in get_nullable_array('communityFeatures', data)]
    self.ConstructionMaterials = [str(x) for x in get_nullable_array('constructionMaterials', data)]
    self.Cooling = [str(x) for x in get_nullable_array('cooling', data)]
    self.Electric = [str(x) for x in get_nullable_array('electric', data)]
    self.ExteriorFeatures = [str(x) for x in get_nullable_array('exteriorFeatures', data)]
    self.FireplaceFeatures = [str(x) for x in get_nullable_array('fireplaceFeatures', data)]
    self.Flooring = [str(x) for x in get_nullable_array('flooring', data)]
    self.FoundationDetails = [str(x) for x in get_nullable_array('foundationDetails', data)]
    self.Gas = [str(x) for x in get_nullable_array('gas', data)]
    self.GreenBuildingVerificationType = [str(x) for x in get_nullable_array('greenBuildingVerificationType', data)]
    self.GreenEnergyEfficient = [str(x) for x in get_nullable_array('greenEnergyEfficient', data)]
    self.GreenWaterConservation = [str(x) for x in get_nullable_array('greenWaterConservation', data)]
    self.Heating = [str(x) for x in get_nullable_array('heating', data)]
    self.LaundryFeatures = [str(x) for x in get_nullable_array('laundryFeatures', data)]
    self.OtherFacts = [str(x) for x in get_nullable_array('otherFacts', data)]
    self.OtherStructures = [str(x) for x in get_nullable_array('otherStructures', data)]
    self.ParkingFeatures = [str(x) for x in get_nullable_array('parkingFeatures', data)]
    self.PatioAndPorchFeatures = [str(x) for x in get_nullable_array('patioAndPorchFeatures', data)]
    self.Rooms = [str(x) for x in get_nullable_array('rooms', data)]
    self.SecurityFeatures = [str(x) for x in get_nullable_array('securityFeatures', data)]
    self.Sewer = [str(x) for x in get_nullable_array('sewer', data)]
    self.SpaFeatures = [str(x) for x in get_nullable_array('spaFeatures', data)]
    self.Utilities = [str(x) for x in get_nullable_array('utilities', data)]
    self.Vegetation = [str(x) for x in get_nullable_array('vegetation', data)]
    self.View = [str(x) for x in get_nullable_array('view', data)]
    self.WaterfrontFeatures = [str(x) for x in get_nullable_array('waterfrontFeatures', data)]
    self.WindowFeatures = [str(x) for x in get_nullable_array('windowFeatures', data)]

    #Dicts
    self.AtAGlanceFacts = data.get('atAGlanceFacts', {})
    self.AdTargets = data.get('adTargets', {})
    self.HomeValues = data.get('homeValues', {})
    self.MortgageRates = data.get('mortgageRates', {})
    self.SolarPotential = data.get('solarPotential', {})
    self.TaxHistory = data.get('taxHistory', {})
    self.NearbyHomes = data.get('nearbyHomes', {})
    self.Schools = data.get('schools', {})
    self.BuildingPermits = data.get('buildingPermits', {})
    self.ListingSubType = data.get('listing_sub_type', {})
    self.PriceHistory = data.get('priceHistory', {})

    # Unknown - Never seen IRL
    self.AboveGradeFinishedArea = str(data['aboveGradeFinishedArea'])
    self.AdditionalParcelsDescription = str(data['additionalParcelsDescription'])
    self.BuildingArea = str(data['buildingArea'])
    self.CarportSpaces = str(data['carportSpaces'])
    self.EntryLevel = str(data['entryLevel'])
    self.FrontageLength = str(data['frontageLength'])
    self.FrontageType = str(data['frontageType'])
    self.GreenIndoorAirQuality = str(data['greenIndoorAirQuality'])
    self.GreenSustainability = str(data['greenSustainability'])
    self.HasElectricOnProperty = str(data['hasElectricOnProperty'])
    self.IsSeniorCommunity = str(data['isSeniorCommunity'])
    self.LotSizeDimensions = str(data['lotSizeDimensions'])
    self.MainLevelBathrooms = str(data['mainLevelBathrooms'])
    self.NumberOfUnitsVacant = str(data['numberOfUnitsVacant'])
    self.OtherParking = str(data['otherParking'])
    self.WaterSources = str(data['waterSources'])
    self.WoodedArea = str(data['woodedArea'])

Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

class Operations:
  def SaveListing(data):
    statement = Listing.__table__.delete().where(Listing.Id.in_([x['zpid'] for x in data]))
    engine.execute(statement)

    session.bulk_save_objects([Listing(x) for x in data])
    session.commit()

  def SaveRentalListing(data):
    statement = RentalListing.__table__.delete().where(RentalListing.Id.in_([x['zpid'] for x in data]))
    engine.execute(statement)

    session.bulk_save_objects([RentalListing(x) for x in data])
    session.commit()


  def SaveListingError(data):
    session.bulk_save_objects([ListingError(x) for x in data])
    session.commit()

  def SaveZIPError(data):
    session.bulk_save_objects([ZIPError(x) for x in data])
    session.commit()

  def QueryZIP():
    return session.query(ZIP).all()

  def SaveLog(data):
    session.add(Log(data))
    session.commit()

if __name__ == "__main__":
  print(os.environ.get('DATABASE'))
