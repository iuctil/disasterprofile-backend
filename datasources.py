#!/usr/bin/python3
import pandas as pd
import os
import json

ds_root = "/mnt/scratch/datasources"

###################################################################################################
#
# American community survey / census
#
# https://www.census.gov/programs-surveys/acs/guidance/which-data-tool/table-ids-explained.html
#

def acs_census(columns=['census_block_group']):
    print("loading census2019 data --- ")

    files = os.scandir(ds_root+"/acs-census/safegraph_open_census_data_2019/data")
    blocks = []
    for file in files:
        print("loading %s" % file.name)
        block = pd.read_csv(ds_root+"/acs-census/safegraph_open_census_data_2019/data/"+file.name, usecols=columns)
        blocks.append(block)

    return pd.concat(blocks)

#state	state_fips	county_fips	county	class_code
def fips():
    print("loading acs-census fips table")
    return pd.read_csv(ds_root+"/acs-census/safegraph_open_census_data_2019/metadata/cbg_fips_codes.csv")
    
#census_block_group	amount_land	amount_water	latitude	longitude
def acsgeo():
    print("loading acs-census geographics data")
    return pd.read_csv(ds_root+"/acs-census/safegraph_open_census_data_2019/metadata/cbg_geographic_data.csv")

def geojson_features():
    print("loading acs-census geojson")
    #with open(ds_root+"/acs-census/safegraph_open_census_data_2010_to_2019_geometry/cbg.geojson") as fp:
    #    geojson = json.load(fp)
    #    return geojson["features"]
    
    df = pd.read_json(ds_root+"/acs-census/safegraph_open_census_data_2010_to_2019_geometry/cbg.geojson")
    print(df)
