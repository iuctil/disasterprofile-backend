#!/usr/bin/python3

import datasources

#df = datasources.acs_census();
#print(df)

fips = datasources.fips()

#lookup fips code from state/county name
print(fips[(fips.state == 'IN') & (fips.county == 'Washington County')])

#lookup state/county name from state/county fips code
print(fips[(fips.state_fips == 18) & (fips.county_fips == 173)])

geo = datasources.acsgeo()

#lookup geographic data for specific census block
info = geo[geo.census_block_group == 10010201001]
print(info.iloc[0].latitude)

#load geojson feature for a specific censusblock
feature = datasources.geojsonByCBG("100010401001")
print(feature)

#load life expectancy
expectancy = datasources.getCDCLifeExpectancy()
info = expectancy[expectancy.index == 1001020100]
print(info)


