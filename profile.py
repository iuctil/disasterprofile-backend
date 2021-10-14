#!/usr/bin/env python3

import us

import datasources
import random #to fake the data
import json

profileDir="profiles"

print("creating profiles")

#ZIP,STCOUNTYFP,CITY,STATE,COUNTYNAME,CLASSFP
#36091,01001,Verbena,AL,Autauga County,H1
zip2fips = datasources.getZIP2FIPS()
for index, row in zip2fips.iterrows():
    #ZIP                     72545
    #STCOUNTYFP               5023
    #CITY            Heber springs
    #STATE                      AR
    #COUNTYNAME    Cleburne County
    #CLASSFP                    H1
    #Name: 2045, dtype: object
 
    print(row.ZIP, row.STCOUNTYFP)

    profile = {
        'zip': row.ZIP, 
        'state': row.STATE,
        'city': row.CITY,
        'county': row.COUNTYNAME,
        'fips': row.STCOUNTYFP,
        'location': {'lat': -11.222, 'lon': 33.444 },
        'hazards': [
            {"hazardId": "flood", "prob": random.random()},
            {"hazardId": "icestorm", "prob": random.random()},
            {"hazardId": "heart-disease", "prob": random.random()},
            {"hazardId": "sinkhole", "prob": random.random()},
            {"hazardId": "tornado", "prob": random.random()},
            {"hazardId": "windstorm", "prob": random.random()},
            {"hazardId": "snowstorm", "prob": random.random()},
        ]
    }
    print(profile)
    with open(profileDir+f'/zip-{row.ZIP}.json', 'w') as outfile:
        json.dump(profile, outfile) 
