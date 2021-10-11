#!/usr/bin/python3
import pandas as pd
import os
import json

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


