#!/usr/bin/env python3

import us

import datasources
#import random #to fake the data
import json #outputs NaN
import simplejson
import sys
import datetime
import os
import time

debug=False

floodRisks = datasources.getFloodRisks()
stormEvents = datasources.getNOAAStormEvents(debug=debug)
leadingCauses = datasources.getCDCLeadingCauseOfDeath()

#for col in leadingCauses.columns:
#    print(col)

#https://stackoverflow.com/questions/1969240/mapping-a-range-of-values-to-another
#def translate(value, leftMin, leftMax, rightMin, rightMax):
#    # Figure out how 'wide' each range is
#    leftSpan = leftMax - leftMin
#    rightSpan = rightMax - rightMin
#
#    # Convert the left range into a 0-1 range (float)
#    valueScaled = float(value - leftMin) / float(leftSpan)
#
#    # Convert the 0-1 range into a value in the right range.
#    return rightMin + (valueScaled * rightSpan)

def handleZip(row):

    stateFullName = datasources.abbrev_to_us_state[row.STATE]
    STATE_FIPS=int(row.STCOUNTYFP/1000)
    CZ_FIPS=row.STCOUNTYFP-(STATE_FIPS*1000)

    print("-----------------------------------------------------------------------")
    print(f"-- fips:{row.STCOUNTYFP} | {stateFullName} | {row.COUNTYNAME} | {row.CITY} | zip:{row.ZIP}")
    print("-----------------------------------------------------------------------")

    hazards = [
        #{"hazardId": "sinkhole", "prob": random.random()},
    ]

    ###########################################################################
    ##
    ## CDC leading cause of death
    ## 
    sourceYear = 2017
    leading = leadingCauses[(leadingCauses.Year == sourceYear) & (leadingCauses.State == stateFullName)]
    allDeath = leading[leadingCauses['Cause Name'] == 'All causes']
    if len(allDeath) != 1:
        print("couldn't find CDC leading cause of death info")
    else:
        totalDeathCount = int(allDeath["Deaths"])
        for rec in leading.iloc:
            cause = rec['Cause Name']
            if cause in datasources.mapCDCCauses2HazardID:
                hazard = {
                    "hazardId": str(datasources.mapCDCCauses2HazardID[cause]),
                    "prob": int(rec['Deaths']) / totalDeathCount, #TODO - how should I compute this?
                    #"desc": f"In the year {sourceYear} there has been {rec['Deaths']} deaths reported (out of total death {totalDeathCount}) in {stateFullName} state.",
                    "source": "CDC-COD",
                    "sourceYear": sourceYear,
                    "aadr": int(rec['Age-adjusted Death Rate']),
                    "deaths": int(rec['Deaths']),
                    "totalDeaths": int(totalDeathCount),
                }
                print(hazard)
                hazards.append(hazard)
            elif cause == "All causes":
                None
            else:
                print("ignoring cdc leading cause", cause)

    #print("leading causes of deaths")
    #print(leading)

    ###########################################################################
    ##
    ## load firststreet floodrisks
    ## 
    floodRiskRows = floodRisks[(floodRisks.zipcode == row.ZIP)]
    #columns------------------------------------
    #count_property                    2850.00
    #count_fema_sfha                     71.00
    #pct_fema_sfha                        2.50
    #count_fs_risk_2020_5               208.00
    #pct_fs_risk_2020_5                   7.30
    #count_fs_risk_2050_5               238.00
    #pct_fs_risk_2050_5                   8.40
    #count_fs_risk_2020_100             369.00
    #pct_fs_risk_2020_100                12.90
    #count_fs_risk_2050_100             381.00
    #pct_fs_risk_2050_100                13.40
    #count_fs_risk_2020_500             410.00
    #pct_fs_risk_2020_500                14.40
    #count_fs_risk_2050_500             422.00
    #pct_fs_risk_2050_500                14.80
    #count_fs_fema_difference_2020      298.00
    #pct_fs_fema_difference_2020         10.50
    #avg_risk_score_all                   2.00
    #avg_risk_score_2_10                  7.75
    #avg_risk_fsf_2020_100                8.34
    #avg_risk_fsf_2020_500                7.92
    #avg_risk_score_sfha                  7.28
    #avg_risk_score_no_sfha               1.87
    #count_floodfactor1                2427.00
    #count_floodfactor2                  13.00
    #count_floodfactor3                   8.00
    #count_floodfactor4                  20.00
    #count_floodfactor5                  14.00
    #count_floodfactor6                 111.00
    #count_floodfactor7                  21.00
    #count_floodfactor8                  10.00
    #count_floodfactor9                  76.00
    #count_floodfactor10                150.00
    #Name: 11386, dtype: float64}

    #TODO - what should I do with it?
    floodRisk = None
    if len(floodRiskRows.index) == 1:
        floodRiskRecord = floodRiskRows.iloc[0]
        floodRisk = floodRiskRecord.to_dict()
    else:
        print("couldn't find floodRisk information")

    ###########################################################################
    ##
    ## count NOAA storm events
    ## 
    events = stormEvents[(stormEvents.STATE_FIPS == STATE_FIPS) & (stormEvents.CZ_FIPS == CZ_FIPS)]
    stormCounts = {}
    for rec in events.iloc:
        #BEGIN_YEARMONTH                                                  201101
        #BEGIN_DAY                                                            20
        #BEGIN_TIME                                                         1653
        #END_YEARMONTH                                                    201101
        #END_DAY                                                              20
        #END_TIME                                                           2000
        #EPISODE_ID                                                        46321
        #EVENT_ID                                                         279052
        #STATE                                                           ALABAMA
        #STATE_FIPS                                                            1
        #YEAR                                                               2011
        #MONTH_NAME                                                      January
        #EVENT_TYPE                                               Winter Weather
        #CZ_TYPE                                                               Z
        #CZ_FIPS                                                               1
        #CZ_NAME                                                      LAUDERDALE
        #WFO                                                                 HUN
        #BEGIN_DATE_TIME                                      20-JAN-11 16:53:00
        #CZ_TIMEZONE                                                       CST-6
        #END_DATE_TIME                                        20-JAN-11 20:00:00
        #INJURIES_DIRECT                                                       0
        #INJURIES_INDIRECT                                                     0
        #DEATHS_DIRECT                                                         0
        #DEATHS_INDIRECT                                                       0
        #DAMAGE_PROPERTY                                                   0.00K
        #DAMAGE_CROPS                                                      0.00K
        #SOURCE                                                          Mesonet
        #MAGNITUDE                                                           NaN
        #MAGNITUDE_TYPE                                                      NaN
        #FLOOD_CAUSE                                                         NaN
        #CATEGORY                                                            NaN
        #TOR_F_SCALE                                                         NaN
        #TOR_LENGTH                                                          NaN
        #TOR_WIDTH                                                           NaN
        #TOR_OTHER_WFO                                                       NaN
        #TOR_OTHER_CZ_STATE                                                  NaN
        #TOR_OTHER_CZ_FIPS                                                   NaN
        #TOR_OTHER_CZ_NAME                                                   NaN
        #BEGIN_RANGE                                                         NaN
        #BEGIN_AZIMUTH                                                       NaN
        #BEGIN_LOCATION                                                      NaN
        #END_RANGE                                                           NaN
        #END_AZIMUTH                                                         NaN
        #END_LOCATION                                                        NaN
        #BEGIN_LAT                                                           NaN
        #BEGIN_LON                                                           NaN
        #END_LAT                                                             NaN
        #END_LON                                                             NaN
        #EPISODE_NARRATIVE     An upper level disturbance along with an arcti...
        #EVENT_NARRATIVE       A dusting to just over one inch of snowfall fe...
        #DATA_SOURCE                                                         CSV

        hazardId = datasources.mapNOAA2HazardID[rec.EVENT_TYPE]

        #print(rec)
        #print(rec.YEAR, rec.EVENT_TYPE)

        if not hazardId in stormCounts:
            stormCounts[hazardId] = {}
        if not rec.YEAR in stormCounts[hazardId]:
            stormCounts[hazardId][int(rec.YEAR)] = 0
        stormCounts[hazardId][int(rec.YEAR)]+=1

    #compute propability of having various storm event each year
    thisyear = datetime.date.today().year
    for TYPE in stormCounts:
        experiencedYears = 0
        for year in range(2010, thisyear):
            if year in stormCounts[TYPE]:
                if stormCounts[TYPE][year] > 0:
                    experiencedYears+=1

        totalYears = thisyear - 2010;
        hazard = {
            "hazardId": str(TYPE), 
            "prob": experiencedYears / totalYears,
            #"desc": f"Your county has experienced {TYPE} events in {experiencedYears} out of the last {totalYears} years",
            "source": "NOAA-STORM-EVENTS",
            "experiencedYears": experiencedYears,
            "totalYears": totalYears,
        }
        hazards.append(hazard)

    ###########################################################################
    ##
    ## Finally put things together
    ## 
    return {
        'zip': row.ZIP, 
        'state': row.STATE,
        'city': row.CITY,
        'county': row.COUNTYNAME,
        'fips': row.STCOUNTYFP,
        'noaa': stormCounts,
        'hazards': hazards,
        #'floodRisk': floodRisk,

        #TODO..
        #'location': {'lat': -11.222, 'lon': 33.444 },
    }

profileDir="profiles"

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
    path = profileDir+f'/zip-{row.ZIP}.json'

    if os.path.exists(path):
        now = time.time()
        old = now - 3600*24*7
        if os.path.getmtime(path) > old:
            print("fresh profile exists.. skipping", path)
            continue

    profile = handleZip(row)
    print(simplejson.dumps(profile, indent=4))

    #print("row")
    #print(floodRiskRows.to_string())
    #print("average risk score")
    #print(floodRisk.avg_risk_score_all) #average risk score of all properties

    #floodRisk = None
    #if len(floodRiskRows.index) == 1:
    #    #floodRisk = translate(floodRiskRows.iloc[0].avg_risk_score_all, 1, 10, 0, 1)
    #    #TODO - I am not sure how to translate avg_risk_score_all to percentile (max is 10.0)
    #    floodRisk = floodRiskRows.iloc[0].avg_risk_score_all/10 
    #    #print(f"average flood risk score of all properties {floodRisk}")

    #print("min", floodRisks.avg_risk_score_all.min())
    #print("max", floodRisks.avg_risk_score_all.max())

    #fips = zip2fips[zip2fips.ZIP == "47403"]
    #print(fips)
    #print(stormEvents[(stormEvents.STATE_FIPS == "18") & (stormEvents.CZ_FIPS == "117")])
    
    with open(path, 'w') as outfile:
        simplejson.dump(profile, outfile, ignore_nan=True, indent=4) 

