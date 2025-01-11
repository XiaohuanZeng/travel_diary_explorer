#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'Summarize counts, duration, distance of trips and activitiies by person day'

__author__ = 'Xiaohuan Zeng'

import numpy as np
import pandas as pd
from functools import reduce

"""
INPUT:  ucalitems_temporal_plot after filtering valid days,

TASKS:  merge trip legs into a trip
        the travel mode is determined by the trip leg with the longest trip distance

OUTPUT: trips with trip legs merged into a single item

"""

def leg2trip(ucalitems_temporal_plot):
    
    # create a new id for complete trips composed of legs, i.e., the new id increases only if the the next episode type changed to activity 
    temp = ucalitems_temporal_plot.sort_values(by=['user_id', 'start_dt'])
    temp['type_decoded_pre'] = temp.groupby(['user_id', 'start_date'])['type_decoded'].shift()
    temp['flag'] = (temp['type_decoded']!='TRIP') | (temp['type_decoded_pre']!='TRIP')
    temp['leg2tripid'] = temp['flag'].cumsum()
    
    # agg to get the sum for each subtype in a complete trip
    duration_sum = temp.groupby(['user_id', 'start_date', 'leg2tripid', 'subtype_decoded']).agg({'distance_after_split': 'sum'}).reset_index()
    
    # keep the longest leg in a complete trip and removing all other records
    leg2trip_index = duration_sum.groupby(['user_id', 'start_date', 'leg2tripid'])['distance_after_split'].idxmax()
    longest_type = duration_sum.loc[leg2trip_index].reset_index(drop=True).drop(columns='distance_after_split')
    
    # agg to get other attributes
    other_attributes = temp.groupby(['user_id', 'start_date', 'leg2tripid']).agg({'distance_after_split': 'sum',
                                                     'duration_after_split': 'sum', 
                                                     'start_timestamp': 'first', 
                                                     'end_timestamp': 'last', 
                                                     'type_decoded': 'first', 
                                                     'survey_not_null': 'max', 
                                                     'start_dt': 'first', 
                                                     'end_dt': 'last', 
                                                     'end_date': 'last', 
                                                     'dow': 'first', 
                                                     'IsWeekend': 'first', 
                                                     'start_time': 'first', 
                                                     'end_time': 'last', 
                                                     'id': 'first'}).reset_index()
    
    
    
    # keep segments attributes
    segment_temp = temp[['user_id', 'start_date', 'leg2tripid', 'subtype_decoded', 'distance_after_split', 'duration_after_split']].copy()
    segment_temp['subtype_decoded'] = segment_temp['subtype_decoded'].replace(' ', '')
    segment_temp['duration_after_split'] = (segment_temp['duration_after_split'] * 60).apply(np.ceil).astype(int).astype(str)
    segment_temp['distance_after_split'] = segment_temp['distance_after_split'].apply(np.ceil).astype(int).astype(str)
    
    segment_attributes = []
    for col in  ['subtype_decoded', 'duration_after_split', 'distance_after_split']: 
        segment_attributes.append(segment_temp.groupby(['user_id', 'start_date', 'leg2tripid'])[col].apply('_'.join).reset_index())
    
    segment_attributes_df = reduce(lambda x, y: pd.merge(x, y, how='inner',  on = ['user_id', 'start_date', 'leg2tripid']), segment_attributes)
    segment_attributes_df.rename(columns = {
        'subtype_decoded': 'segment_subtype', 
        'duration_after_split': 'segment_duration_minute', 
        'distance_after_split': 'segment_distance_meter', 
    }, inplace=True)
    
    # merge tables to get results
    result = reduce(lambda x, y: pd.merge(x, y, how='inner',  on = ['user_id', 'start_date', 'leg2tripid']), 
                   [longest_type, other_attributes, segment_attributes_df])
    
    print('# rows before leg2trip: {}. # rows after leg2trip: {}'.format(str(temp.shape[0]), str(result.shape[0])))
    
    return(result)

"""
INPUT:  csv dict with selected valid dates

OUTPUT: pandas dataframe with 'Median', 'Mean', 'SD', 'Min', 'Max' values for 
        trip, activity counts, duration, distance, activity space measures

"""
def overview_statistics(csv_dict, stat_group_cols = ['IsWeekend', 'Statistics']): 
    per_day = []

    # groupby person_day
    group_cols = ['user_id', 'start_date']
    
    # Activity Space by person_day
    agg_list0 = [['convex_hull', 'area_mile', '8_Convex Hull Area (Square Miles)'], 
                ['sde', 'area_mile', '90_Ellipse Area (Square Miles)'],
                 ['sde', 'sx_mile', '91_Ellipse semi-major Axis (Miles)'], 
                ['sde', 'sy_mile', '92_Ellipse semi-minor Axis (Miles)']]
    
    for item in agg_list0:
        per_day_duration = csv_dict[item[0]].copy()[['user_id', 'start_date', item[1]]].copy()
        per_day_duration.rename(columns={item[1]: 'value'}, inplace=True)
        per_day_duration['Statistics'] = item[2]
        per_day.append(per_day_duration)
    
    # hours of no-off data
    per_day_duration = csv_dict['day_summary'].copy()[['user_id', 'start_date', 'no_off']].copy()
    per_day_duration.rename(columns={'no_off': 'value'}, inplace=True)
    per_day_duration['value'] = per_day_duration['value']*60
    per_day_duration['Statistics'] = '1_Recorded Data per Day (Minutes)'
    per_day.append(per_day_duration)
    
    # daily summaries for activity and trip
    agg_list = [ 
                ['ACTIVITY', 'duration_after_split', 'sum', 60, '2_Total Activity Duration per Day (Minutes)', 'ucalitems_ljoin_ucisurvey_split'], 
                ['TRIP', 'duration_after_split', 'sum', 60, '3_Total Trip Duration per Day (Minutes)', 'ucalitems_ljoin_ucisurvey_split'], 
                # [['OFF', 'DATA COLLECTION STARTED', 'INACC'], 'duration_after_split', 'sum', 60, '4_Total Missing Data (Minutes)'], 
                ['TRIP', 'distance_after_split', 'sum', 0.000621371, '5_Total Trip Distance per Day (Miles)', 'ucalitems_ljoin_ucisurvey_split'], 
                ['ACTIVITY', 'id', 'count', 1, '6_Activity Count per Day', 'ucalitems_ljoin_ucisurvey_split'], 
                ['TRIP', 'id', 'count', 1, '70_Trip (Segment) Count per Day', 'ucalitems_ljoin_ucisurvey_split'], 
                ['TRIP', 'id', 'count', 1, '71_Trip (Complete) Count per Day', 'leg2trip']
               ]

    for item in agg_list: 
        
        tb = item[-1]
        types = item[0]
        df_sub = csv_dict[tb].query("type_decoded==@types")    

        per_day_item = df_sub.groupby(group_cols).agg({item[1]: item[2]})
        per_day_item.reset_index(inplace=True)
        per_day_item.rename(columns={item[1]: 'value'}, inplace=True)
        per_day_item = pd.merge(left=per_day_item, right=per_day_duration[['user_id', 'start_date']], on=['user_id', 'start_date'], how='right')

        per_day_item['value'] = per_day_item['value'].fillna(0)
        per_day_item['value'] = per_day_item['value']*item[3]
        per_day_item['Statistics'] = item[4]

        per_day.append(per_day_item)

    per_day_df = pd.concat(per_day) # concat all results into a long table
    
    # extract data infor, DOW
    per_day_df['date_new'] = pd.to_datetime(per_day_df['start_date'])
    per_day_df['date_new'] = pd.to_datetime(per_day_df['date_new'].dt.date)
    per_day_df['dow_num'] = per_day_df['date_new'].dt.dayofweek
    per_day_df['dow'] = per_day_df['date_new'].dt.day_name()
    per_day_df['IsWeekend'] = per_day_df['date_new'].dt.dayofweek > 4
    
    # summarize the median, mean, std, min, max
    result = per_day_df.groupby(stat_group_cols)['value'].agg(['median', 'mean', np.std, 'min', 'max']).reset_index()
    # rename the description for the statistics
    result['Statistics'] = result['Statistics'].str.split('_').str[1]
    
    result.iloc[0:2, result.columns.get_loc('max')] = 1440 # round the maximum value to 1440
    
    if stat_group_cols == ['Statistics']:
        result.columns = ['Statistics', 'Median', 'Mean', 'SD', 'Min', 'Max'] # rename columns
    elif stat_group_cols == ['IsWeekend', 'Statistics']:
        result.columns = ['IsWeekend', 'Statistics', 'Median', 'Mean', 'SD', 'Min', 'Max'] # rename columns
    else:
        raise Exception("Sorry, stat_group_cols parameter not correct")
    
    return(result)

if __name__=='__main__':
    pass