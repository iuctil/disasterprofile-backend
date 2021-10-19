#!/usr/bin/env python3

import us

import datasources
import random #to fake the data
import json
import sys

floodRisks = datasources.getFloodRisks()

profileDir="profiles"

print("creating profiles")

#https://stackoverflow.com/questions/1969240/mapping-a-range-of-values-to-another
def translate(value, leftMin, leftMax, rightMin, rightMax):
    # Figure out how 'wide' each range is
    leftSpan = leftMax - leftMin
    rightSpan = rightMax - rightMin

    # Convert the left range into a 0-1 range (float)
    valueScaled = float(value - leftMin) / float(leftSpan)

    # Convert the 0-1 range into a value in the right range.
    return rightMin + (valueScaled * rightSpan)

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
 
    print(f"zip: {row.ZIP} stcountyfips:{row.STCOUNTYFP}")

    floodRiskRows = floodRisks[(floodRisks.zipcode == row.ZIP)]
    #print("row")
    #print(floodRisk)
    #print("average risk score")
    #print(floodRisk.avg_risk_score_all) #average risk score of all properties
    #sys.exit(1)
    floodRisk = None
    if len(floodRiskRows.index) == 1:
        floodRisk = translate(floodRiskRows.iloc[0].avg_risk_score_all, 1, 10, 0, 1)
        print(f"average flood risk score of all properties {floodRisk}")

    #print("min", floodRisks.avg_risk_score_all.min())
    #print("max", floodRisks.avg_risk_score_all.max())

    profile = {
        'zip': row.ZIP, 
        'state': row.STATE,
        'city': row.CITY,
        'county': row.COUNTYNAME,
        'fips': row.STCOUNTYFP,
        'location': {'lat': -11.222, 'lon': 33.444 },
        'hazards': [
            {"hazardId": "flood", "prob": floodRisk},
            {"hazardId": "icestorm", "prob": random.random()},
            {"hazardId": "heart-disease", "prob": random.random()},
            {"hazardId": "sinkhole", "prob": random.random()},
            {"hazardId": "tornado", "prob": random.random()},
            {"hazardId": "windstorm", "prob": random.random()},
            {"hazardId": "snowstorm", "prob": random.random()},
        ]
    }
    #print(profile)
    with open(profileDir+f'/zip-{row.ZIP}.json', 'w') as outfile:
        json.dump(profile, outfile) 
