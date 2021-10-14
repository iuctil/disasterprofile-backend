#!/usr/bin/python3

import pandas as pd
import os
import json
import glob

###################################################################################################
#
# American community survey / census
#
# https://www.census.gov/programs-surveys/acs/guidance/which-data-tool/table-ids-explained.html
#

def acs_census(columns=['census_block_group']):
    print("loading census2019 data --- ")

    files = os.scandir("/mnt/scratch/datasources/acs-census/safegraph_open_census_data_2019/data")
    blocks = []
    for file in files:
        print("loading %s" % file.name)
        block = pd.read_csv("/mnt/scratch/datasources/acs-census/safegraph_open_census_data_2019/data/"+file.name, usecols=columns)
        blocks.append(block)

    return pd.concat(blocks)

#TODO - deprecated by pip us module?
#state	state_fips	county_fips	county	class_code
def fips():
    print("loading acs-census fips table")
    return pd.read_csv("/mnt/scratch/datasources/acs-census/safegraph_open_census_data_2019/metadata/cbg_fips_codes.csv")
    
#census_block_group	amount_land	amount_water	latitude	longitude
def acsgeo():
    print("loading acs-census geographics data")
    return pd.read_csv("/mnt/scratch/datasources/acs-census/safegraph_open_census_data_2019/metadata/cbg_geographic_data.csv")

# search by census block group
def geojsonByCBG(cbg):
    print("loading acs-census geojson %s" % cbg)
    prefix=cbg[0:3]

    path = "/mnt/scratch/datasources/acs-census/safegraph_open_census_data_2010_to_2019_geometry/blocks/%s/%s.feature.json" % (prefix, cbg)
    if os.path.exists(path):
        return pd.read_json(path)
    else:
        print("can't find geojson feature for cbg:%s at %s" % (cbg, path))
        return None

def getCDCLifeExpectancy():
    print("loading cdc life expectancy")
    return pd.read_csv("/mnt/scratch/datasources/cdc-life-expectancy/US_B.CSV", index_col='Tract ID')

#columns
#femaDeclarationString,disasterNumber,state,declarationType,declarationDate,fyDeclared,incidentType,declarationTitle,ihProgramDeclared,iaProgramDeclared,paProgramDeclared,hmProgramDeclared,incidentBeginDate,incidentEndDate,disasterCloseoutDate,fipsStateCode,fipsCountyCode,placeCode,designatedArea,declarationRequestNumber,hash,lastRefresh,id
def getFEMADisasterDeclarations():
    print("loading fema disaster declerations")
    decls = pd.read_csv("/mnt/scratch/datasources/fema/csvfiles/DisasterDeclarationsSummaries.csv")
    decls.set_index(['fipsStateCode', 'fipsCountyCode'])
    return decls

#columns
#BEGIN_YEARMONTH,BEGIN_DAY,BEGIN_TIME,END_YEARMONTH,END_DAY,END_TIME,EPISODE_ID,EVENT_ID,STATE,STATE_FIPS,YEAR,MONTH_NAME,EVENT_TYPE,CZ_TYPE,CZ_FIPS,CZ_NAME,WFO,BEGIN_DATE_TIME,CZ_TIMEZONE,END_DATE_TIME,INJURIES_DIRECT,INJURIES_INDIRECT,DEATHS_DIRECT,DEATHS_INDIRECT,DAMAGE_PROPERTY,DAMAGE_CROPS,SOURCE,MAGNITUDE,MAGNITUDE_TYPE,FLOOD_CAUSE,CATEGORY,TOR_F_SCALE,TOR_LENGTH,TOR_WIDTH,TOR_OTHER_WFO,TOR_OTHER_CZ_STATE,TOR_OTHER_CZ_FIPS,TOR_OTHER_CZ_NAME,BEGIN_RANGE,BEGIN_AZIMUTH,BEGIN_LOCATION,END_RANGE,END_AZIMUTH,END_LOCATION,BEGIN_LAT,BEGIN_LON,END_LAT,END_LON,EPISODE_NARRATIVE,EVENT_NARRATIVE,DATA_SOURCE
def getNOAAStormEvents():
    year=2010
    print("loading NOAA data since 2010")
    blocks = []
    for path in glob.glob("/mnt/scratch/datasources/noaa-stormevent-death/csvfiles/StormEvents_details-ftp_v1.0_d20[12]*"):
        print(path)
        blocks.append(pd.read_csv(path))
    print("concatenating")
    return pd.concat(blocks)

def getZIP2FIPS():
    return pd.read_csv("/mnt/scratch/datasources/zip2fips/zip2fips.csv")
