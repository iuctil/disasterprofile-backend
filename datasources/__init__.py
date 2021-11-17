#!/usr/bin/python3

import pandas as pd
import os
import json
import glob

import pandas as pd
from sodapy import Socrata

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

def getCDCLeadingCauseOfDeath():
    #https://data.cdc.gov/profile/edit/developer_settings
    #apitoken="w7x08fgiw6sixsc0g5hql7oqhahslc0gkwb22v5f76pgllgsm"
    #https://data.cdc.gov/resource/bi63-dtpu.json
    #url="https://data.cdc.gov/resource/bi63-dtpu.json?year=2017"

    #client = Socrata("data.cdc.gov", None)
    #results = client.get("bi63-dtpu", where="year = 2017 and state = 'Indiana'", limit=2000)
    #return pd.DataFrame.from_records(results)

    #Columns
    #Year
    #113 Cause Name
    #Cause Name
    #State
    #Deaths
    #Age-adjusted Death Rate
    print("loading cdc leading causes of death")
    return pd.read_csv("/mnt/scratch/datasources/cdc-leading-causes-death/rows.csv")

#columns
#femaDeclarationString,disasterNumber,state,declarationType,declarationDate,fyDeclared,incidentType,declarationTitle,ihProgramDeclared,iaProgramDeclared,paProgramDeclared,hmProgramDeclared,incidentBeginDate,incidentEndDate,disasterCloseoutDate,fipsStateCode,fipsCountyCode,placeCode,designatedArea,declarationRequestNumber,hash,lastRefresh,id
#
#TODO - I think we should directly query fema api
#  https://www.fema.gov/about/openfema/api
#  for example https://www.fema.gov/api/open/v1/DisasterDeclarationsSummaries?$filter=state%20eq%20%27VA%27
#
def getFEMADisasterDeclarations():
    print("loading fema disaster declerations")
    decls = pd.read_csv("/mnt/scratch/datasources/fema/csvfiles/DisasterDeclarationsSummaries.csv")
    decls.set_index(['fipsStateCode', 'fipsCountyCode'])
    return decls

#columns
#BEGIN_YEARMONTH,BEGIN_DAY,BEGIN_TIME,END_YEARMONTH,END_DAY,END_TIME,EPISODE_ID,EVENT_ID,STATE,STATE_FIPS,YEAR,MONTH_NAME,EVENT_TYPE,CZ_TYPE,CZ_FIPS,CZ_NAME,WFO,BEGIN_DATE_TIME,CZ_TIMEZONE,END_DATE_TIME,INJURIES_DIRECT,INJURIES_INDIRECT,DEATHS_DIRECT,DEATHS_INDIRECT,DAMAGE_PROPERTY,DAMAGE_CROPS,SOURCE,MAGNITUDE,MAGNITUDE_TYPE,FLOOD_CAUSE,CATEGORY,TOR_F_SCALE,TOR_LENGTH,TOR_WIDTH,TOR_OTHER_WFO,TOR_OTHER_CZ_STATE,TOR_OTHER_CZ_FIPS,TOR_OTHER_CZ_NAME,BEGIN_RANGE,BEGIN_AZIMUTH,BEGIN_LOCATION,END_RANGE,END_AZIMUTH,END_LOCATION,BEGIN_LAT,BEGIN_LON,END_LAT,END_LON,EPISODE_NARRATIVE,EVENT_NARRATIVE,DATA_SOURCE
def getNOAAStormEvents(debug=False):
    print("loading noaa storm events")
    blocks = []
    for path in glob.glob("/mnt/scratch/datasources/noaa-stormevent-death/csvfiles/StormEvents_details-ftp_v1.0_d20[12]?_*"):
        print(path)
        blocks.append(pd.read_csv(path))

        if debug:
            print("only loading first one - for debug")
            break

    print("concatenating storm events")
    return pd.concat(blocks)

def getZIP2FIPS():
    return pd.read_csv("/mnt/scratch/datasources/zip2fips/zip2fips.csv")

#Summary Column Header	Description
#count_property	number of properties
#count_fema_sfha	number of properties in FEMA SHFA
#pct_fema_sfha	percent of properties in FEMA SFHA
#count_fs_risk_2020_5	number of First Street properties with flooding in the 2020 Return Period 5 scenario
#pct_fs_risk_2020_5	percent of First Street  properties with flooding in the 2020 Return Period 5 scenario
#count_fs_risk_2050_5	number of First Street properties with flooding in the 2050 Return Period 5 scenario
#pct_fs_risk_2050_5	percent of First Street properties with flooding in the 2050 Return Period 5 scenario
#count_fs_risk_2020_100	number of First Street properties with flooding in the 2020 Return Period 100 scenario
#pct_fs_risk_2020_100	percent of First Street properties with flooding in the 2020 Return Period 100 scenario
#count_fs_risk_2050_100	number of First Street properties with flooding in the 2050 Return Period 100 scenario
#pct_fs_risk_2050_100	percent of First Street properties with flooding in the 2050 Return Period 100 scenario
#count_fs_risk_2020_500	number of First Street properties with flooding in the 2020 Return Period 500 scenario
#pct_fs_risk_2020_500	percent of First Street properties with flooding in the 2020 Return Period 500 scenario
#count_fs_risk_2050_500	number of First Street properties with flooding in the 2050 Return Period 500 scenario
#pct_fs_risk_2050_500	percent of First Street properties with flooding in the 2050 Return Period 500 scenario
#count_fs_fema_difference_2020	absolute difference in properties at risk between First Street and FEMA in 2020
#pct_fs_fema_difference_2020	percent difference between number of First Street properties and FEMA properties at risk  in 2020
#avg_risk_score_all	average risk score of all properties
#avg_risk_score_2_10	average risk scores from 2-10, excluding 1 (minimal risk)
#avg_risk_fsf_2020_100	average risk score of properties with flooding in the 2020 RP 100 scenario
#avg_risk_fsf_2020_500	average risk score of properties with flooding in the 2020 RP 500 scenario
#avg_risk_score_sfha	average risk score of properties in a FEMA SFHA
#avg_risk_score_no_sfha	average risk score of properties not in a FEMA SFHA
#count_floodfactor1	number of properties with a risk score = 1
#count_floodfactor2	number of properties with a risk score = 2
#count_floodfactor3	number of properties with a risk score = 3
#count_floodfactor4	number of properties with a risk score = 4
#count_floodfactor5	number of properties with a risk score = 5
#count_floodfactor6	number of properties with a risk score = 6
#count_floodfactor7	number of properties with a risk score = 7
#count_floodfactor8	number of properties with a risk score = 8
#count_floodfactor9	number of properties with a risk score = 9
#count_floodfactor10	number of properties with a risk score = 10
def getFloodRisks():
    print("loading flood risks")
    return pd.read_csv("/mnt/scratch/datasources/first-street-climate-risk-statistics/01_DATA/Climate_Risk_Statistics/v1.3/Zip_level_risk_FEMA_FSF_v1.3.csv")

us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI",
}
    
# invert the dictionary
abbrev_to_us_state = dict(map(reversed, us_state_to_abbrev.items()))

mapNOAA2HazardID = {
    "High Wind": "highwind",
    "Strong Wind": "highwind",

    "Heat": "heat",
    "Excessive Heat": "heat",

    "Frost/Freeze": "frostfreeze",
    "Thunderstorm Wind": "thunderstorm",

    "Hail": "hailstorm",
    "Sleet": "sleet",
    "Freezing Fog": "freezingfog",
    "Dense Fog": "densefog",

    "Flood": "flood",
    "Flash Flood": "flashflood",
    "Coastal Flood": "coastalflood",
    "High Surf": "highsurf",
    "Lakeshore Flood": "lakeshoreflood",
    "Storm Surge/Tide": "stormsurge",
    "High Surf": "highsurf",
    "Tsunami": "tsunami",

    "Extreme Cold/Wind Chill": "extremecold",
    "Cold/Wind Chill": "extremecold",

    "Ice Storm": "icestorm",
    "Winter Storm": "winterstorm",
    "Winter Weather": "winterweather",
    "Blizzard": "blizzard",
    "Heavy Snow": "heavysnow",
    "Heavy Rain": "heavyrain",
    "Avalanche": "avalanche",

    "Lake-Effect Snow": "lakeeffectsnow",

    "Lightning": "lightning",
    "Dust Devil": "duststorm",
    "Dust Storm": "duststorm",

    "Funnel Cloud": "funnelcloud",
    "Tornado": "tornado",

    "Drought": "drought",
    "Seiche": "seiche",

    "Tropical Depression": "tropicaldepression",
    "Tropical Storm": "tropicalstorm",

    "Hurricane": "hurricane",
    "Hurricane (Typhoon)": "hurricane",

    "Wildfire": "wildfire",

    "Landslide": "landslide",
    "Debris Flow": "debrisflow",

    "Rip Current": "ripcurrent",

    "Dense Smoke": "smoke",
    "Sneakerwave": "sneakerwave",
    "Astronomical Low Tide": "lowtide",
    "Volcanic Ashfall": "ashfall",
}

mapCDCCauses2HazardID = {
    "Unintentional injuries": "injuries", 
    "Alzheimer's disease": "alzheimer",
    "Stroke": "stroke",
    "CLRD": "clrd",
    "Diabetes": "diabetes",
    "Heart disease": "heart-disease",
    "Influenza and pneumonia": "influenza",
    "Suicide": "suicide",
    "Cancer": "cancer",
    "Kidney disease": "kidney-disease",
}
