#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'Preprocess data'

__author__ = 'Xiaohuan Zeng'

import numpy as np
import pandas as pd


"""
INPUT: ucalitems_suvery, ucalitems <two tables in the data dictionary create from S1_read_data.py>
       
OUTPUT: a joined table "ucalitems_ljoin_ucisurvey" based on 'ucalitems' and 'ucalitems_suvery'
"""

def ucalitems_ljoin_ucisurvey(ucalitems_suvery, ucalitems):
    
    ucalitems_suvery_agg = ucalitems_suvery.query('response==response').copy().groupby(['user_id', 'calendar_item_id', 'calendar_item_timestamp'])[['question_id']].agg('count').reset_index()
    
    # create shorter names for future modules. Please note that "question_id", "calendar_item_id", and "calendar_item_timestamp" are original column names
    ucalitems_suvery_agg.rename(columns={'question_id': 'survey_not_null', 
                             'calendar_item_id': 'cal_item_id', 
                             'calendar_item_timestamp': 'start_timestamp'}, inplace=True)
    
    ucalitems_suvery_agg['survey_not_null'] = True
    
    print('# rows of original survey data: {}. # rows after aggregation: {}'.format(str(ucalitems_suvery.shape[0]), str(ucalitems_suvery_agg.shape[0])))

    # join the ucalitems with calendar_item_survey as a master table ucalitems_split_survey
    result = pd.merge(
        left = ucalitems, 
        right = ucalitems_suvery_agg, 
        on = ['user_id', 'cal_item_id', 'start_timestamp'], 
        how = 'left'
    )
    result['survey_not_null'] = result['survey_not_null'].fillna(False)

    print('# rows of original ucalitems: {}. # rows after left join: {}'.format(str(ucalitems.shape[0]), str(result.shape[0])))
    print('# ucalitems with survey (True) and without survey (False): ')
    print(result['survey_not_null'].value_counts().sort_index(ascending=False))
    
    return(result)


"""
INPUT: ucalitems_ljoin_ucisurvey <the master joint table after running the function ucalitems_ljoin_ucisurvey>, 
        local_timezone <the local time zone, e.g. US/Central for Minnesota>, 
        unix_time_unit <the time unit used in the original timestamp, 'ms' is the current default>, need to update if original data change formats> 
        min_time_stamp <the time stamp used to filter records with default setting, the default is (the current default timestamp-10000000000)>
        
TASKS
    - split multi-day data
    - label calendar items with user interactions: new columns 'interact_with_app', 'interact_by_confirm', 'interact_by_edit'

OUTPUT: a new table 'ucalitems_ljoin_ucisurvey_split' with split days and new labels for various types of user interactions with app

"""

def split_ucalitems(ucalitems_ljoin_ucisurvey, local_timezone, unix_time_unit = 'ms', min_time_stamp=0):
    
    # preprocess before splitting days
    # converting string to datetime objects with timezone and deal with time zones
    ucalitems_ljoin_ucisurvey['start_dt'] = pd.to_datetime(ucalitems_ljoin_ucisurvey['start_timestamp'], unit=unix_time_unit, utc=True).dt.tz_convert(local_timezone)
    ucalitems_ljoin_ucisurvey['end_dt'] = pd.to_datetime(ucalitems_ljoin_ucisurvey['end_timestamp'], unit=unix_time_unit, utc=True).dt.tz_convert(local_timezone)

    ucalitems_ljoin_ucisurvey['start_date'] = pd.to_datetime(ucalitems_ljoin_ucisurvey['start_dt'].dt.date)
    ucalitems_ljoin_ucisurvey['end_date'] = pd.to_datetime(ucalitems_ljoin_ucisurvey['end_dt'].dt.date)

    ucalitems_ljoin_ucisurvey['days'] = (ucalitems_ljoin_ucisurvey['end_date']-ucalitems_ljoin_ucisurvey['start_date']).dt.days + 1

    ucalitems_ljoin_ucisurvey = ucalitems_ljoin_ucisurvey.query('days>0')
    ucalitems_ljoin_ucisurvey.sort_values(by=['user_id', 'start_dt'], inplace=True)
    ucalitems_ljoin_ucisurvey['id'] = range(ucalitems_ljoin_ucisurvey.shape[0])
    ucalitems_ljoin_ucisurvey['duration_before_split'] = (ucalitems_ljoin_ucisurvey['end_dt'] - ucalitems_ljoin_ucisurvey['start_dt']) / pd.Timedelta(hours=1)
    
    # create duplicated lines for multiple days
    split_days = pd.concat([pd.Series(r.id, pd.date_range(r.start_date, r.end_date, freq='D')) 
                 for r in ucalitems_ljoin_ucisurvey.itertuples()]).reset_index()
    split_days.columns = ['start_date','id']
    split_days.sort_values(by=['id', 'start_date'], inplace=True)
    
    # outer join; duplicated records will have null values for many columns after the join
    result = pd.merge(split_days, ucalitems_ljoin_ucisurvey, on=['id', 'start_date'], how='left')
    result.sort_values(by=['id', 'start_date'], inplace=True)
    cols = ucalitems_ljoin_ucisurvey.isna().sum()[ucalitems_ljoin_ucisurvey.isna().sum() <= 0].index.tolist()
    cols.remove('start_dt')
    # print(cols, len(cols), len(result.columns.tolist()))
    result[cols]=result[cols].ffill()

    # update the start and end date time of splitted days; end time set as 23:59:59, start time of the next day set as 00:00:00
    result['diff'] = result['start_date'].dt.date - result['end_date'].dt.date
    result.end_dt[result['start_date'].dt.date != result['end_date'].dt.date] = result['end_dt'].dt.normalize().copy() + result['diff'].copy() + pd.Timedelta(days=1) - pd.Timedelta(seconds=0.001)  
    result.start_dt[result['start_dt'].isna()] = result['end_dt'].shift() + pd.Timedelta(seconds=0.001)

    # update the duration of calendar item after splitting
    result['duration_after_split'] = (result['end_dt'] - result['start_dt']) / pd.Timedelta(hours=1)
    result['end_date'] = result['end_dt'].dt.date
    result['distance_after_split'] = result['duration_after_split'] / result['duration_before_split'] * result['distance']
    result['distance_after_split'] = result['distance_after_split'].fillna(0)

    # print(result.isna().sum())
    result.drop(columns = ['days', 'diff'], inplace=True)
    result['dow'] = result['start_date'].dt.day_name()
    
    print('# rows of original ucalitems: {0:0.0f}. # rows after splitting: {1:0.0f}'.format(ucalitems_ljoin_ucisurvey.shape[0], result.shape[0]))
    print('# hours in  original ucalitems: {0:0.2f}. # hours after splitting: {1:0.2f}'.format(ucalitems_ljoin_ucisurvey['duration_before_split'].sum(), result['duration_after_split'].sum()))
    print('# distance in  original ucalitems: {0:0.2f}. # distance after splitting: {1:0.2f}'.format(ucalitems_ljoin_ucisurvey['distance'].sum(), result['distance_after_split'].sum()))
    
    #for each calendar item, create labels for user interaction, set the default value for the label as 0, if satisfied, change label to 1
    #create 3 labels to select person day with user interactions: "confirm_timestamp" and "edit_timestamp"
    result['interact_with_app'] = (result['confirm_timestamp']>min_time_stamp) | (result['edit_timestamp']>min_time_stamp) #any interaction
    result['interact_by_confirm'] = (result['confirm_timestamp']>min_time_stamp)   #confirm calendar item
    result['interact_by_edit'] = (result['edit_timestamp']>min_time_stamp)         #edit calendar item

    return(result)

"""
INPUT: ucalitems_ljoin_ucisurvey_split <master table with multi-day splitted after running the function split_ucalitems above>

TASKS: for each day of a given person (a.k.a. person-day), summarize
    - total hours of data
    - total hours with device on
    - total count of interactions via editing OR confirming
    - total count of interactions via editing ONLY
    - total count of interactions via confirmation ONLY
    - total hours with in-app survey filled out
    - total counts and hours of trips
    - total counts and hours of activities
    
OUTPUT: a day-level summary table 'day_summary' 
"""

def get_per_day_duration(df):
    
    per_day = []

    mask_dict = {
    'total': ~df['user_id'].isna(), 
    'no_off': df['type_decoded'].isin(['ACTIVITY', 'TRIP']), # the same as df['subtype']!='UNKNOWN'
    
    'interact_with_app': df['interact_with_app']==True, 
    'interact_by_confirm': df['interact_by_confirm']==True,
    'interact_by_edit': df['interact_by_edit']==True, 
    
    'with_subtype': (~df['subtype_decoded'].isin(['ACTIVITY', 'TRIP'])) & df['type_decoded'].isin(['ACTIVITY', 'TRIP']),  
    'with_survey': (df['survey_not_null']==True) | (df['subtype_decoded']=='HOME'), # for our example survey design, 
                                                                                    # users do not require to enter survey for home activities
    # 'with_survey': df['survey_not_null']==True),  # if your survey design REQUIRE user to fill in-app survey for home activities, 
                                                    # please use this one instead of the line above 
        
    'trip_count': df['type_decoded']=='TRIP', 
    'trip_duration': df['type_decoded']=='TRIP', 
    
    'activity_count': df['type_decoded']=='ACTIVITY', 
    'activity_duration': df['type_decoded']=='ACTIVITY', 
    }
    
    for key, value in mask_dict.items():
        group_cols = ['user_id', 'dow', 'start_date']
        df_sub = df[value]
        print(key, df_sub.shape)
        agg_func = 'sum'
        if key in ['interact_with_app', 'interact_by_confirm', 'interact_by_edit', 'trip_count', 'activity_count']: 
            agg_func = 'count'
        per_day_duration = df_sub.groupby(group_cols).agg({'duration_after_split': agg_func})
        per_day_duration.reset_index(inplace=True)
        per_day_duration.rename(columns={'duration_after_split': 'hours'}, inplace=True)
        per_day_duration['stat_type'] = key
        per_day.append(per_day_duration)
    
    result = pd.concat(per_day).pivot(index=['user_id', 'dow', 'start_date'], columns='stat_type', values='hours')
    result.reset_index(inplace=True)
    # print(result.isna().sum())
    result.fillna(0, inplace=True)
    # print(result.isna().sum())
    
    # create a new column to indicate whether that day (for a person) is during weekend or not
    result["IsWeekend"] = result['start_date'].dt.dayofweek > 4
    result["IsWeekend"] = result["IsWeekend"].map({True:'Weekend', False:'Weekday'})
    
    return(result)

if __name__=='__main__':
    pass