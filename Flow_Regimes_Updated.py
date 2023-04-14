#-------------------------------------------------------------------------------
# Name:        Flow Regime Assignment Script
# Purpose:     Assigning a flow regime of perennial, intermittent or ephemeral to existing WBID segments
#              or bucketing inconclusive data in a undetermined category
#
# Author:      34617, 184819, and Joseph Gutenson
#
# Created:     12/03/2018
# Updated:     03/07/2023
# Copyright:   (c) 34617 & 184819 2018
# Licence:
#-------------------------------------------------------------------------------
# -*- coding: utf-8 -*-

import geopandas as gpd
from collections import defaultdict
from datetime import datetime, timedelta
import logging

if __name__ == '__main__':

    # path to the flow regimes geodatabase
    FlowGDB_path = r"C:\2023_NHD_Nevada\Scripts\ADEQ_FlowRegimes\ADEQ_FlowRegimes\Flow_Regimes.gdb"

    # fields in the flow observation dataset
    flds = ["WBID","ReachLength_Mi","Priority","Scope_of_Observation","Length","FLOW_REGIM","RECON_YEAR","Obs_Type","Wet_Dry"]

    # path to the output log file that tracks results
    log_file_path = r"C:\2023_NHD_Nevada\Scripts\ADEQ_FlowRegimes\ADEQ_FlowRegimes\FlowRegimes2.log"

    # path to the output shapefile with the designated flow regimes
    flowline_output_path = r"C:\2023_NHD_Nevada\Scripts\ADEQ_FlowRegimes\ADEQ_FlowRegimes\Flow_Regimes.shp"

    ### end of specified variables

    # start logging the time
    start_time = datetime.now()

    # load in the flow observation and flowline GIS data
    FlowObs_gdf = gpd.read_file(FlowGDB_path, layer='FlowRegime_Observations')
    FlowObs_gdf = FlowObs_gdf[flds]
    FlowObs_WBID_list = FlowObs_gdf['WBID'].to_list()
    FlowDesg_gdf = gpd.read_file(FlowGDB_path, layer='WBID_FlwRgme_Designations')

    # remove fields in flowline gdf that have no observations
    FlowDesg_filtered_gdf = FlowDesg_gdf[FlowDesg_gdf['WBID'].isin(FlowObs_WBID_list)]

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(filename = log_file_path, filemode = 'w', level=logging.INFO)


    start_time = datetime.now()



    print("Start time: " + str(start_time))

    tmp = {}
    flag = 0



    print("Creating Flow Regimes...")

    i=0

    print("Populating dictionary with WBIDs, Reach Lengths...")

    tmpObsDict= {row['WBID']: {'TotalLength':round(row['Length_Mi'],2), 'TotalCount': None,'Priority': None,'CntPriority': None,
                'Scope': None, 'SumLength': 0.0, 'Status': None, 'FlowRegime': None, 'Flag': None, 'Most Recent': None,
                'PSum': 0.0, 'ISum': 0.0, 'ESum': 0.0,'PSumR': 0.0, 'ISumR': 0.0, 'ESumR': 0.0, 'Obs_Type': 0, 'ReserveFR': None} for index, row in FlowDesg_filtered_gdf.iterrows()}

    logging.info(len(tmpObsDict))


    #count observation records
    for keyA in tmpObsDict.keys():
        cntobs = 0
        FlowObs_filtered_gdf = FlowObs_gdf.loc[FlowObs_gdf['WBID']==str(keyA)]
        cntobs = len(FlowObs_filtered_gdf.index)
        tmpObsDict[keyA]['TotalCount'] = cntobs


    # Determine controlling priority, scope, and count of controlling priority records
    # Populate remainder of dictionary

    print("Populating remainder of dictionary, sorting for high priority...")

    hipriority = 99
    fr =''

    cntprobs = 0
    Mostrecent = 0
    year = 0
    frmr = ''
    for keyA in tmpObsDict.keys():
        reservefr = None    #if keyA == 'AZ15020008-020':
         #   print "Here..."
            #print keyA
        FlowObs_filtered_gdf = FlowObs_gdf.loc[FlowObs_gdf['WBID']==str(keyA)]
        # print(FlowObs_filtered_gdf)
        for index, row in FlowObs_filtered_gdf.iterrows():
            if keyA == row[0] and row[5]!=None:        #check for matching key and non-null flow regime
                priority = row[2]
                obs_type = row[7]      #added -test
                scope = row[3]
                tmpObsDict[keyA]['Obs_Type']  = obs_type

                # row[5] is the flow regime observed in the observations
                # if the scope is reach level and the flow regime is not blank
                # fill the flowline dataset with the observed flow regime
                if str(scope) == 'RCH' and (row[5] != None and row[5]!='') :
                    if tmpObsDict[keyA]['FlowRegime'] == None:
                        tmpObsDict[keyA]['FlowRegime'] = row[5]
                    else:
                        reservefr = tmpObsDict[keyA]['FlowRegime']


                # if the observation type is 7 (site visits) and the flow regime is not blank
                # check the year when the observation was made to determine if the observations are from the lastest Year
                # the latest year's results will override previous years observations
                # if multiple observations exist for the latest year, the sum length (row[4]) of each flow regime class is summed
                if obs_type == 7 and (row[5]!= None and row[5]!=''):
                    year = row[6]                                              ##Site visit processing
                    if Mostrecent == year:
                        tmpObsDict[keyA]['Flag'] = 2
                        if tmpObsDict[keyA]['FlowRegime'] == 'P':
                            if tmpObsDict[keyA]['PSum']!= None:
                                tmpObsDict[keyA]['PSum'] = tmpObsDict[keyA]['PSum'] + row[4]
                            else:
                                 tmpObsDict[keyA]['PSum'] = row[4]
                        elif tmpObsDict[keyA]['FlowRegime'] == 'I':
                            if tmpObsDict[keyA]['ISum']!= None:
                                tmpObsDict[keyA]['ISum'] = tmpObsDict[keyA]['ISum'] + row[4]
                            else:
                                tmpObsDict[keyA]['ISum'] = row[4]
                        elif tmpObsDict[keyA]['FlowRegime'] == 'E':
                            if tmpObsDict[keyA]['ESum']!= None:
                                tmpObsDict[keyA]['ESum'] = tmpObsDict[keyA]['ESum'] + row[4]
                            else:
                                tmpObsDict[keyA]['ISum'] = row[4]
                    if Mostrecent < year:
                        Mostrecent = year
                        frmr = row[5]
                        tmpObsDict[keyA]['Most Recent'] = Mostrecent        #update Most recent in Dictionary for staff site visits
                        tmpObsDict[keyA]['FlowRegime'] = frmr             #update associated Flow regime for most recent visit
                        if tmpObsDict[keyA]['FlowRegime'] == 'P':
                            tmpObsDict[keyA]['PSum'] = row[4]
                        elif tmpObsDict[keyA]['FlowRegime'] == 'I':
                            tmpObsDict[keyA]['ISum'] = row[4]
                        elif tmpObsDict[keyA]['FlowRegime'] == 'E':
                            tmpObsDict[keyA]['ESum'] = row[4]

                # if the priority of the observation is less than the high priority specified by the script user
                # run this loop
                if priority < hipriority:
                    if hipriority != 99:
                        reservefr =  tmpObsDict[keyA]['FlowRegime']
                    cntprobs = 0
                    next_hi_pr = hipriority
                    hipriority = priority
                    scope = row[3]

                    # if the scope of the observation is reach but somehow the at the top of this loop was skipped
                    # reset the flow regime specified by the reach level observation
                    if scope == 'RCH' and tmpObsDict[keyA]['ReserveFR'] != reservefr:
                       tmpObsDict[keyA]['ReserveFR'] = reservefr
                    if scope == 'RCH' and tmpObsDict[keyA]['FlowRegime'] != row[5]:
                       tmpObsDict[keyA]['FlowRegime'] = row[5]


                    # Calculate/assign Sumlength according to scope of record
                    # if the scope of the observation is segement, the length is not None, and the length of all observations
                    # odd to the sum length of observations
                    if (scope == 'SEG' or scope =='DYNSEG') and row[4] != None and tmpObsDict[keyA]['SumLength'] != row[1]:
                        tmpObsDict[keyA]['SumLength']= 0.0
                        if tmpObsDict[keyA]['SumLength'] != None:
                            tmpObsDict[keyA]['SumLength'] = tmpObsDict[keyA]['SumLength'] + row[4]
                        else:
                            tmpObsDict[keyA]['SumLength'] = row[4]
                    elif scope == "RCH":
                        pass
                    else:
                        pass

                    # if the observation assigns a flow regime, count it
                    if (row[5] != None):
                        fr = row[5]
                        cntprobs+=1
                    else:
                        pass

                    # If the observation type is anything other than 7 (observations other than site visits)
                    # Sum the lengths of each flow duration classification in the observation together
                    if obs_type != 7:              #Added-test
                        # Summation of segment lengths by flow regime
                        if row[5] =='P'and row[4]!= None and tmpObsDict[keyA]['PSumR'] != row[1]:
                            if  tmpObsDict[keyA]['PSumR'] != None:
                                tmpObsDict[keyA]['PSumR'] = tmpObsDict[keyA]['PSumR'] + row[4]
                            else:
                                tmpObsDict[keyA]['PSumR'] = row[4]
                        elif row[5] =='I'and row[4]!= None and tmpObsDict[keyA]['ISumR'] != row[1]:
                            if  tmpObsDict[keyA]['ISumR'] != None:
                                tmpObsDict[keyA]['ISumR'] = tmpObsDict[keyA]['ISumR'] + row[4]
                            else:
                                tmpObsDict[keyA]['ISumR'] = row[4]
                        elif row[5] =='E'and row[4]!= None and tmpObsDict[keyA]['ESumR'] != row[1]:
                            if  tmpObsDict[keyA]['ESumR'] != None:
                                tmpObsDict[keyA]['ESumR'] = tmpObsDict[keyA]['ESumR'] + row[4]
                            else:
                                tmpObsDict[keyA]['ESumR'] = row[4]
                        else:
                            pass
                    else:
                        pass

                # if the priority of the observation is equal to the high priority specified by the script user
                # run this loop
                elif priority == hipriority:
                    cntprobs+=1
                    scope = row[3]
                    # Calculate/assign Sumlength according to scope of record
                    if (scope == 'SEG' or scope =='DYNSEG') and row[4] != None and tmpObsDict[keyA]['SumLength'] != row[1]:
                        if tmpObsDict[keyA]['SumLength'] != None:
                            tmpObsDict[keyA]['SumLength'] = tmpObsDict[keyA]['SumLength'] + row[4]
                        else:
                            tmpObsDict[keyA]['SumLength'] = row[4]
                    elif scope == "RCH":
                        if tmpObsDict[keyA]['Scope'] != "RCH":
                            tmpObsDict[keyA]['ReserveFR'] =  tmpObsDict[keyA]['FlowRegime']
                            tmpObsDict[keyA]['FlowRegime'] =  row[5]
                            tmpObsDict[keyA]['Scope'] = scope
                    else:
                        pass
                    if fr != row[5] and (row[5] != None and row[5] != ''):
                        flag = 1
                    else:
                        fr = row[5]
                    # For observations other than site visits
                    if obs_type != 7:
                        #Summation of segment lengths by flow regime
                        if row[5] =='P'and row[4]!= None and tmpObsDict[keyA]['PSumR'] != row[1]:
                            if  tmpObsDict[keyA]['PSumR'] != None:
                                tmpObsDict[keyA]['PSumR'] = tmpObsDict[keyA]['PSumR'] + row[4]
                            else:
                                tmpObsDict[keyA]['PSumR'] = row[4]
                        elif row[5] =='I'and row[4]!= None and tmpObsDict[keyA]['ISumR'] != row[1]:
                            if  tmpObsDict[keyA]['ISumR'] != None:
                                tmpObsDict[keyA]['ISumR'] = tmpObsDict[keyA]['ISumR'] + row[4]
                            else:
                                tmpObsDict[keyA]['ISumR'] = row[4]
                        elif row[5] =='E'and row[4]!= None and tmpObsDict[keyA]['ESumR'] != row[1]:
                            if  tmpObsDict[keyA]['ESumR'] != None:
                                tmpObsDict[keyA]['ESumR'] = tmpObsDict[keyA]['ESumR'] + row[4]
                            else:
                                tmpObsDict[keyA]['ESumR'] = row[4]
                        else:
                            pass
                    else:
                        pass


                else:
                    continue


            # Determine flow regime ratios to reach length for segment summations
            if tmpObsDict[keyA]['SumLength'] != 0 and tmpObsDict[keyA]['SumLength']!= None:
                PRatio = tmpObsDict[keyA]['PSumR']/tmpObsDict[keyA]['SumLength']
                IRatio = tmpObsDict[keyA]['ISumR']/tmpObsDict[keyA]['SumLength']
                ERatio = tmpObsDict[keyA]['ESumR']/tmpObsDict[keyA]['SumLength']

            # assign remaining attributes to main dictionary
            tmpObsDict[keyA]['Priority'] = hipriority
            tmpObsDict[keyA]['Scope'] = scope
            tmpObsDict[keyA]['CntPriority'] = cntprobs
            tmpObsDict[keyA]['SumLength'] = round(tmpObsDict[keyA]['SumLength'],2)
            if tmpObsDict[keyA]['Most Recent'] == None:
                tmpObsDict[keyA]['FlowRegime'] = fr
            else:
                pass
            if  tmpObsDict[keyA]['Flag'] != 2:
                tmpObsDict[keyA]['Flag'] = flag
            # Site visit updated priority
            if tmpObsDict[keyA]['Priority'] == 3:
                tmpObsDict[keyA]['Most Recent'] = Mostrecent
            if flag != 1:
                if scope =='RCH':
                    tmpObsDict[keyA]['Status'] = 'Complete'
                else:
                    tmpObsDict[keyA]['Status'] = 'Partial'
            else:
                tmpObsDict[keyA]['Status'] = "Processed, Complete"

                if obs_type!= 7:              ##Added- test
                    if tmpObsDict[keyA]['SumLength'] != 0.0 and PRatio > IRatio and PRatio > ERatio and PRatio > 0.5:
                            tmpObsDict[keyA]['FlowRegime'] = 'P'
                            logging.info("{0}  P Ratio:  {1}".format(keyA, round(PRatio,3)))
                    elif tmpObsDict[keyA]['SumLength'] != 0.0 and IRatio > PRatio and IRatio > ERatio and IRatio > 0.5:
                            tmpObsDict[keyA]['FlowRegime'] = 'I'
                            logging.info("{0}  I Ratio:  {1}".format(keyA, round(IRatio,3)))
                    elif tmpObsDict[keyA]['SumLength'] != 0.0 and ERatio > PRatio and ERatio > IRatio and ERatio > 0.5:
                            tmpObsDict[keyA]['FlowRegime'] = 'E'
                            logging.info("{0}  E Ratio:  {1}".format(keyA, round(ERatio,3)))
                    else:
                            tmpObsDict[keyA]['FlowRegime'] = 'U'
                            logging.info("{0}  Max Ratio:   {1}     Status: {2}".format(keyA, round(max(PRatio, IRatio, ERatio),3), tmpObsDict[keyA]['FlowRegime']))
                #elif hipriority == 3 and tmpObsDict[keyA]['Flag'] == 2:
                elif obs_type== 7 and tmpObsDict[keyA]['Flag'] == 2:       ##Added-test
                    if tmpObsDict[keyA]['PSum']>tmpObsDict[keyA]['ISum'] and tmpObsDict[keyA]['PSum']>tmpObsDict[keyA]['ESum']:
                        tmpObsDict[keyA]['FlowRegime'] = 'P'
                    elif tmpObsDict[keyA]['ISum']>tmpObsDict[keyA]['PSum'] and tmpObsDict[keyA]['ISum']>tmpObsDict[keyA]['ESum']:
                        tmpObsDict[keyA]['FlowRegime'] = 'I'
                    elif tmpObsDict[keyA]['ESum']>tmpObsDict[keyA]['PSum'] and tmpObsDict[keyA]['ESum']>tmpObsDict[keyA]['ISum']:
                        tmpObsDict[keyA]['FlowRegime'] = 'E'
                    logging.info("{0} Most Recent Year: {1}  Multiple Most Recent Obs, Max Length: {2}  Flow Regime: {3}".format(keyA, tmpObsDict[keyA]['Most Recent'],
                                max(tmpObsDict[keyA]['PSum'],tmpObsDict[keyA]['ISum'], tmpObsDict[keyA]['ESum']), tmpObsDict[keyA]['FlowRegime']))
                else:
                    logging.info("{0} Most Recent Year: {1}  Flow Regime Most Recently:  {2}".format(keyA, Mostrecent, frmr))


            # these variables are reset after cycling through each observation
            hipriority= 99
            SumLength = 0.0
            fr = ''
            cntprobs = 0
            flag = 0
            Mostrecent = 0
            frmr = ''

    # At this point, the observations are set as we want them to be
    for keyA in tmpObsDict.keys():
        # if the flow flow regime remains undetermined for the observation
        # set the flow regime to unknown for the observation
        if tmpObsDict[keyA]['FlowRegime'] == None or tmpObsDict[keyA]['FlowRegime'] =='':
            tmpObsDict[keyA]['FlowRegime'] = 'U'
        if tmpObsDict[keyA]['Status'] == 'Partial':
            # if observation is less than 50% of the reach length, set the status of the flow regime
            # classifcation to complete for the observation to complete
            # but the flow regime is still unknown for the reach.
            if tmpObsDict[keyA]['SumLength']/tmpObsDict[keyA]['TotalLength']  < 0.5:
                tmpObsDict[keyA]['FlowRegime'] = 'U'
                tmpObsDict[keyA]['Status'] = 'Complete'

    for keyA in tmpObsDict.keys():
        # if the flow regime is currently set to unknown but their is a reserve flow regime saved from the observation processing
        # replace the unknown flow regime with the flow regime that was reserved during observation processing
        if tmpObsDict[keyA]['FlowRegime'] == 'U' and tmpObsDict[keyA]['ReserveFR']!= None and tmpObsDict[keyA]['ReserveFR']!='':
            tmpObsDict[keyA]['FlowRegime'] = tmpObsDict[keyA]['ReserveFR']

    try:
        # this process begins assessing the observations for various complexities and problems
        # and catalouging those locations and problems
        print("Generating subset dictionaries for log file...")
        logging.info(len(tmpObsDict))

        newdict1 = {k: val for (k, val) in tmpObsDict.items() if tmpObsDict[k]['TotalCount']<tmpObsDict[k]['CntPriority'] }
        logging.info("Printing newdict1...Records with Priority count higher than total count")
        for key, value in newdict1.items():
            logging.info(key, value)
        logging.info( "Size, newdict1: {0}".format(len(newdict1)))


        newdict2 = {k: val for (k, val) in tmpObsDict.items() if tmpObsDict[k]['TotalLength']<tmpObsDict[k]['SumLength'] and tmpObsDict[k]['SumLength'] != 'N.A.' }
        logging.info("Printing newdict2...Records where SumLength is greater than total length")
        for key, value in newdict2.items():
            #for item in value:
                logging.info( key, value)
        logging.info( "Size, newdict2: {0}".format(len(newdict2)))

        newdict3 = {k: val for (k, val) in tmpObsDict.items() if tmpObsDict[k]['Flag']==1 }
        logging.info("Printing newdict3...Mixed flow regime records")
        for key, value in newdict3.items():
            logging.info(key, value)
        logging.info( "Size, newdict3: {0}".format(len(newdict3)))

        ##newdict4 = {k: val for (k, val) in tmpObsDict.items() if tmpObsDict[k]['Priority'] == 1 }
        ##logging.info("Printing newdict4...Wet-dry mapping reaches")
        ##for key, value in newdict4.iteritems():
        ##    logging.info(key, value)
        ##logging.info( "Size, newdict4: {0}".format(len(newdict4)))

        #newdict5 = {k: val for (k, val) in newdict3.items() if not isinstance(newdict3[k]['FlowRegime'], unicode)}
        newdict5 = {k: val for (k, val) in newdict3.items() if tmpObsDict[k]['Status']=='Complete' or tmpObsDict[k]['Status']=='Processed, Complete'}
        logging.info("Printing newdict5...processed mixed reaches")
        for key, value in newdict5.items():
                logging.info(key, value)
        logging.info( "Size, newdict5: {0}".format(len(newdict5)))

        newdict6 = {k: val for (k, val) in newdict3.items() if tmpObsDict[k]['Status']!='Complete' and tmpObsDict[k]['Status']!='Processed, Complete'}
        logging.info("Printing newdict6...unprocessed mixed reaches")
        for key, value in newdict6.items():

                logging.info(key, value)
        logging.info( "Size, newdict6: {0}".format(len(newdict6)))

        newdict7 = {k: val for (k, val) in tmpObsDict.items() if tmpObsDict[k]['Status']!='Partial'}
        logging.info("Printing newdict7...partial processed reaches")
        for key, value in newdict7.items():

                logging.info(key, value)
        logging.info( "Size, newdict7: {0}".format(len(newdict7)))

    except Exception as e:
        print("Failed to generate subset dictionaries")
        print(e)


    print("Calculating statistics for Wet-dry mapped reaches...")
    try:
        tmpWDMpDict = dict()
        i=0
        print("Summarizing Wet-dry mapped statistics...")
        logging.info( "Wet-dry summary log:")
        # looping through the observations to determine if the
        # observation can be designated perennial or intermittent based upon
        # wet dry designation
        Wetsum = 0.0
        Drysum = 0.0
        pr1fr = "U"
        for keyA in tmpObsDict.keys():
            i+=1
            if i % 2 != 0:
                WBID = keyA
                Year = tmpObsDict[keyA]['Most Recent']
                if row[8] == "Dry":
                    Drysum = round(tmpObsDict[WBID]['SumLength'],4)
                elif row[8] == "Wet":
                    Wetsum = round(tmpObsDict[WBID]['SumLength'],4)
            else:
                if row[8] == "Dry":
                    Drysum = round(tmpObsDict[WBID]['SumLength'],4)
                elif row[8] == "Wet":
                    Wetsum = round(tmpObsDict[WBID]['SumLength'],4)
                # print("WBID: {0}, Year:  {1}, Wet Sum: {2}, Dry Sum {3}, Total Length: {4}".format(WBID, Year, Wetsum, Drysum, tmpObsDict[WBID]['TotalLength']))
                if Wetsum > Drysum and Wetsum/tmpObsDict[keyA]['TotalLength'] > 0.5:
                    pr1fr = 'P'
                elif Drysum > Wetsum and Drysum/tmpObsDict[keyA]['TotalLength'] > 0.5:
                    pr1fr = 'I'
                else:
                    pr1fr = 'U'

                if WBID in tmpWDMpDict.keys():
                    if (WBID, pr1fr) in tmpWDMpDict.items():
                        pass
                    elif pr1fr == 'pI' and pr1fr != tmpWDMpDict.values:
                        tmpWDMpDict[WBID] = pr1fr
                    else:
                        pass
                else:
                    tmpWDMpDict[WBID] = pr1fr
            logging.info("Final Determination = WBID: {0}, Year:  {1}, Wet Sum: {2}, Dry Sum {3}, Total Length: {4}, Flow Regime: {5}".format(WBID, Year, Wetsum, Drysum, tmpObsDict[WBID]['TotalLength'], pr1fr))
    except Exception as e:
        print("Failed to calculate, summarize Wet-dry mapped Statistics")
        print(e)
        print("\n")

    print("Updating master dictionary with Wet-Dry mapping attributes...")
    # using wet dry observation to designate a reach as P or I
    try:
        for key, value in tmpWDMpDict.items():
            if tmpObsDict[key]['FlowRegime'] !=None and tmpObsDict[key]['Scope'] != 'RCH' and tmpObsDict[key]['Status']!='Complete':           ###Restrict W-D mapping results to reaches only where RCH scope not achieved and FR not assigned
                tmpObsDict[key]['FlowRegime'] = value
                tmpObsDict[key]['Priority'] = 2
                tmpObsDict[key]['Scope'] = u'DYNSEG'
                tmpObsDict[key]['CntPriority'] = tmpObsDict[key]['TotalCount'] - tmpObsDict[key]['CntPriority']
                tmpObsDict[key]['Status'] = "PR1 Processed, Complete"
                logging.info("{0}   {1}".format(key,tmpObsDict[key].items()))

        newdict4 = {k: val for (k, val) in tmpObsDict.items() if tmpObsDict[k]['Obs_Type'] == 10 }
        logging.info("Printing newdict4...Wet-dry mapping reaches")
        for key, value in newdict4.items():
            #for item in value:
            logging.info(key, value)
        logging.info( "Size, newdict4: {0}".format(len(newdict4)))

    except Exception as e:
        print("Failed to calculate, summarize Wet-Dry mapping Statistics")
        print(e)
        sys.exit(0)

    print("Final pass...resolving undetermined reaches where possible")
    for keyA in tmpObsDict.keys():
        if tmpObsDict[keyA]['FlowRegime'] == 'U' and tmpObsDict[keyA]['ReserveFR']!= None:
            tmpObsDict[keyA]['FlowRegime'] = tmpObsDict[keyA]['ReserveFR']
        #if keyA == 'AZ15050203-004C':
         #   print tmpObsDict[keyA]



    print("Updating FlowRegime Designations layer...")
    # merging the results back with the flowline dataset and outputting as a new shapefile
    try:
        z = 0
        PCnt = 0
        ICnt = 0
        ECnt = 0
        UCnt = 0
        for index, row in FlowDesg_gdf.iterrows():
            for key in tmpObsDict:
                if row['WBID'] == key:
                    if row['IsEstablished'] == 'T':
                        pass
                    else:

                        row['Flow_Regime'] = tmpObsDict[key]['FlowRegime']
                        if tmpObsDict[key]['FlowRegime'] == 'P':
                            PCnt +=1
                        elif tmpObsDict[key]['FlowRegime'] == 'I':
                            ICnt +=1
                        elif tmpObsDict[key]['FlowRegime'] == 'E':
                            ECnt +=1
                        elif tmpObsDict[key]['FlowRegime'] == 'U':
                            UCnt +=1
                        if tmpObsDict[key]['Flag'] == 1:
                            if row['Comments'] == None:
                                row['Comments'] = "Mixed flow regime observations"
                            elif 'Mixed flow regime observations' in row['Comments']:
                                pass
                            else:
                                row['Comments'] = row['Comments'] + "; Mixed flow regime observations"
                        # cursor.updateRow(row)
                        z+=1
                        logging.info( "Updated row: {0}  {1}".format(key, row['Flow_Regime']))
                else:
                    continue
            logging.info("Count Ps: {0}\tCount Is: {1}\tCount Es: {2}\tCount Us: {3}".format(PCnt,ICnt,ECnt,UCnt))
            logging.info( "Total updates: {0}".format(z))

        FlowDesg_gdf.to_file(flowline_output_path)

        stop_time = datetime.now()
        elapsed_time = stop_time - start_time
        print("End time: " + str(datetime.now()))
        print("Elapsed time: ", elapsed_time)

    except Exception as e:
        print("Failed to update Flow Regime designations")
        print(e)
