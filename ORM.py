import os
import json
from datetime import datetime
from sqlalchemy import ForeignKey, desc, create_engine, func, Column, BigInteger, JSON, Integer, Float, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker


engine = create_engine(os.environ.get('DATABASE'), echo=False)
Base = declarative_base()

def get_nullable_array(array_name, dictionary):
  if dictionary.get(array_name, []) != None:
    return dictionary.get(array_name, []) 
  else:
    return []

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
    self.ZPID = data['zpid']
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
  OnMarketDate = Column('onMarketDate', DateTime, nullable=True)
  ScrapeDate = Column('scrapeDate', DateTime, nullable=True)
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
  CityRegion = Column('cityRegion', String, nullable=True)
  CommonWalls = Column('commonWalls', String, nullable=True)
  ConstructionMaterials = Column('constructionMaterials', String, nullable=True)
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
  StructureType = Column('structureType', String, nullable=True)
  Topography = Column('topography', String, nullable=True)
  VirtualTour = Column('virtualTour', String, nullable=True)
  Zoning = Column('zoning', String, nullable=True)
  ZoningDescription = Column('zoningDescription', String, nullable=True)
  Bathrooms = Column('bathrooms', Integer, nullable=True)
  BathroomsFull = Column('bathroomsFull', Integer, nullable=True)
  BathroomsHalf = Column('bathroomsHalf', Integer, nullable=True)
  BathroomsOneQuarter = Column('bathroomsOneQuarter', Integer, nullable=True)
  BathroomsPartial = Column('bathroomsPartial', Integer, nullable=True)
  BathroomsThreeQuarter = Column('bathroomsThreeQuarter', Integer, nullable=True)
  Bedrooms = Column('bedrooms', Integer, nullable=True)
  CoveredSpaces = Column('coveredSpaces', Integer, nullable=True)
  Fireplaces = Column('fireplaces', Integer, nullable=True)
  GarageSpaces = Column('garageSpaces', Integer, nullable=True)
  NumberOfUnitsInCommunity = Column('numberOfUnitsInCommunity', Integer, nullable=True)
  OpenParkingSpaces = Column('openParkingSpaces', Integer, nullable=True)
  Parking = Column('parking', Integer, nullable=True)
  Stories = Column('stories', Integer, nullable=True)
  StoriesTotal = Column('storiesTotal', Integer, nullable=True)
  TaxAnnualAmount = Column('taxAnnualAmount', Integer, nullable=True)
  TaxAssessedValue = Column('taxAssessedValue', Integer, nullable=True)
  YearBuilt = Column('yearBuilt', Integer, nullable=True)
  ZIP = Column('zip', Integer, nullable=True)
  YearBuiltEffective = Column('yearBuiltEffective', Integer, nullable=True)
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


  def __init__(self, data):
    # Id
    self.Id = data['zpid']

    # Dates
    if data['onMarketDate'] != None:
      self.OnMarketDate = datetime.fromtimestamp(data['onMarketDate']/1000)
    else:
      self.OnMarketDate = None

    self.ScrapeDate = datetime.now()
    self.ZIP = data['zip']

    # Strings
    self.ArchitecturalStyle = data['architecturalStyle']
    self.AssociationFee = data['associationFee']
    self.AssociationFee2 = data['associationFee2']
    self.AssociationName = data['associationName']
    self.AssociationName2 = data['associationName2']
    self.AssociationPhone = data['associationPhone']
    self.AssociationPhone2 = data['associationPhone2']
    self.Basement = data['basement']
    self.BelowGradeFinishedArea = data['belowGradeFinishedArea']
    self.BuilderModel = data['builderModel']
    self.BuilderName = data['builderName']
    self.BuildingAreaSource = data['buildingAreaSource']
    self.BuildingName = data['buildingName']
    self.CityRegion = data['cityRegion']
    self.CommonWalls = data['commonWalls']
    self.ConstructionMaterials = data['constructionMaterials']
    self.DevelopmentStatus = data['developmentStatus']
    self.ElementarySchool = data['elementarySchool']
    self.ElementarySchoolDistrict = data['elementarySchoolDistrict']
    self.EntryLocation = data['entryLocation']
    self.Fencing = data['fencing']
    self.HighSchool = data['highSchool']
    self.HighSchoolDistrict = data['highSchoolDistrict']
    self.HomeType = data['homeType']
    self.IsNewConstruction = data['isNewConstruction']
    self.LandLeaseAmount = data['landLeaseAmount']
    self.Levels = data['levels']
    self.ListingId = data['listingId']
    self.LivingArea = data['livingArea']
    self.LotSize = data['lotSize']
    self.MiddleOrJuniorSchool = data['middleOrJuniorSchool']
    self.MiddleOrJuniorSchoolDistrict = data['middleOrJuniorSchoolDistrict']
    self.OtherStructures = data['otherStructures']
    self.ParcelNumber = data['parcelNumber']
    self.PropertyCondition = data['propertyCondition']
    self.RoofType = data['roofType']
    self.StructureType = data['structureType']
    self.Topography = data['topography']
    self.VirtualTour = data['virtualTour']
    self.Zoning = data['zoning']
    self.ZoningDescription = data['zoningDescription']

    # Integers
    self.Bathrooms = data['bathrooms']
    self.BathroomsFull = data['bathroomsFull']
    self.BathroomsHalf = data['bathroomsHalf']
    self.BathroomsOneQuarter = data['bathroomsOneQuarter']
    self.BathroomsPartial = data['bathroomsPartial']
    self.BathroomsThreeQuarter = data['bathroomsThreeQuarter']
    self.Bedrooms = data['bedrooms']
    self.CoveredSpaces = data['coveredSpaces']
    self.Fireplaces = data['fireplaces']
    self.GarageSpaces = data['garageSpaces']
    self.NumberOfUnitsInCommunity = data['numberOfUnitsInCommunity']
    self.OpenParkingSpaces = data['openParkingSpaces']
    self.Parking = data['parking']
    self.Stories = data['stories']
    self.StoriesTotal = data['storiesTotal']
    self.TaxAnnualAmount = data['taxAnnualAmount']
    self.TaxAssessedValue = data['taxAssessedValue']
    self.YearBuilt = data['yearBuilt']
    self.YearBuiltEffective = data['yearBuiltEffective']

    # Booleans
    self.CanRaiseHorses = data['canRaiseHorses']
    self.Furnished = data['furnished']
    self.HasAdditionalParcels = data['hasAdditionalParcels']
    self.HasAssociation = data['hasAssociation']
    self.HasAttachedGarage = data['hasAttachedGarage']
    self.HasAttachedProperty = data['hasAttachedProperty']
    self.HasCarport = data['hasCarport']
    self.HasCooling = data['hasCooling']
    self.HasFireplace = data['hasFireplace']
    self.HasGarage = data['hasGarage']
    self.HasHeating = data['hasHeating']
    self.HasHomeWarranty = data['hasHomeWarranty']
    self.HasLandLease = data['hasLandLease']
    self.HasOpenParking = data['hasOpenParking']
    self.HasPetsAllowed = data['hasPetsAllowed']
    self.HasPrivatePool = data['hasPrivatePool']
    self.HasRentControl = data['hasRentControl']
    self.HasSpa = data['hasSpa']
    self.HasView = data['hasView']
    self.HasWaterfrontView = data['hasWaterfrontView']
    self.IsNewConstruction = data['isNewConstruction']

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

  def SaveListingError(data):
    session.bulk_save_objects([ListingError(x) for x in data])
    session.commit()

  def SaveZIPError(data):
    session.bulk_save_objects([ZIPError(x) for x in data])
    session.commit()

  def QueryZIP():
    return session.query(ZIP).all()

if __name__ == "__main__":
  print(os.environ.get('DATABASE'))
