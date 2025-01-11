#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'Count and filter valid days'

__author__ = 'Xiaohuan Zeng'

import pandas as pd
import geopandas as gpd

"""
INPUT:  day_summary table in the data dictionary from S2_preprocess_data.py
        numerator_col <column in the table used to defined valid days, the default is 'with_subtype' - at least one activity or trip with subtype>
        denominator_filter <days used as the baseline, the default are days with at least one confirmed OR edited item>

OUTPUT: table to show the count of valid days and all days
"""
def count_valid_per_days(day_summary, numerator_col = 'with_subtype', denominator_filter = 'interact_with_app > 0'): 
    
    df = day_summary.query(denominator_filter)
    result_list = []
    
    # day of the week, used as the rows for the output table
    dow_lsit = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Total']
    
    # minimum hours to define valid days, include 24, 20, 16, 12, 8 hours, used as the columns for the output table
    # can be changed into other durations based on the specific project preferences 
    threshold_list = [24, 20, 16, 12, 8]
    threshold_list = [(x-0.01) for x in threshold_list]
    
    for dow in dow_lsit: 
        result_item = [dow]
        total_days = None
        if dow=='Total': 
            total_days = df.shape[0]
            for threshold in threshold_list:
                sub_days = df.query('(`{0}` >=@threshold)'.format(numerator_col)).shape[0]
                if total_days==0:
                    result_item.append('nan')
                else:            
                    sub_days_per = sub_days / total_days * 100
                    result_item.append('{:,} ({:.1f}%)'.format(sub_days, sub_days_per))    
        
        else: 
            total_days = df.query('dow==@dow').shape[0]
            for threshold in threshold_list:
                sub_days = df.query('(`{0}` >=@threshold)&(dow==@dow)'.format(numerator_col)).shape[0]
                if total_days==0:
                    result_item.append('nan')
                else: 
                    sub_days_per = sub_days / total_days * 100
                    result_item.append('{:,} ({:.1f}%)'.format(sub_days, sub_days_per))

        # result_item.append('{:,}'.format(total_days))
        # result_item.append(str(int(total_days)))
        result_item.append(total_days)
        result_list.append(result_item)

    columns = ['Day of the Week'] + ['# of days with more than {:.0f} hours of data'.format(i) for i in threshold_list] + ['Total # of Days']
    result_df = pd.DataFrame(result_list, columns = columns)
    return(result_df)


"""
INPUT: csv_dict <data dictionary after S2 but before applying the filtering function above>,
       tb <ONE table to be filtered based on the day_summary table>, 
       query_text <filtering condition, text expressions>, 
       cols <keys/columns used for merging>
       
OUTPUT: ONE filtered table by filter condition
"""
def query_valid_days_func(csv_dict, tb, query_text, cols = ['user_id', 'start_date']):
    df = pd.merge(
        left = csv_dict[tb], 
        right = csv_dict['day_summary'].query(query_text)[cols], 
        on = ['user_id', 'start_date'], 
        how = 'inner'
    )

    print('Table Name: {0}. # items before filtering: {1:0.0f}. # items after filtering: {2:0.0f}'.format(tb, csv_dict[tb].shape[0], df.shape[0]))
    return(df)


"""
INPUT: csv_dict_origin <original data dictionary after S2 but before applying the filtering function above>,
       query_text      <filtering/selecting condition, text-based expressions>, 

TASKS: call function query_valid defined above to handle multiple tables in the data dictionary using the same filtering condition

OUTPUT: several filtered tables saved in a new dictionary of tables by filter condition 
"""
def filter_valid_days(csv_dict_origin, query_text):
    
    csv_dict_sub = {}  #output data dictionary with subset of records in each table in the original dictionary
    
    # 0. day_summary filtered by query_text
    csv_dict_sub['day_summary'] = csv_dict_origin['day_summary'].query(query_text)
    print('# days before filtering: {0:0.0f}. # days after filtering: {1:0.0f}'.format(csv_dict_origin['day_summary'].shape[0], csv_dict_sub['day_summary'].shape[0]))

    # 1. ucalitems_ljoin_ucisurvey_split filtered by query_text
    csv_dict_sub['ucalitems_ljoin_ucisurvey_split'] = query_valid_days_func(csv_dict_origin, tb='ucalitems_ljoin_ucisurvey_split', query_text=query_text)
    csv_dict_sub['ucalitems_ljoin_ucisurvey_split']["IsWeekend"] = csv_dict_sub['ucalitems_ljoin_ucisurvey_split']['start_date'].dt.dayofweek > 4
    csv_dict_sub['ucalitems_ljoin_ucisurvey_split']["IsWeekend"] = csv_dict_sub['ucalitems_ljoin_ucisurvey_split']["IsWeekend"].map({True:'Weekend', False:'Weekday'})
    
    # 2. ucalitems_temporal_plot, a copy of ucalitems_ljoin_ucisurvey for creating plots 
    #    with formated time and decoded activity and trip types & subtypes
    tb_plot = 'ucalitems_temporal_plot'
    
    csv_dict_sub[tb_plot] = csv_dict_sub['ucalitems_ljoin_ucisurvey_split'].copy()
    csv_dict_sub[tb_plot]["IsWeekend"] = csv_dict_sub[tb_plot]['start_date'].dt.dayofweek > 4
    csv_dict_sub[tb_plot]["IsWeekend"] = csv_dict_sub[tb_plot]["IsWeekend"].map({True:'Weekend', False:'Weekday'})
    csv_dict_sub[tb_plot]['start_time'] = csv_dict_sub[tb_plot]['start_dt'].dt.time.astype(str)
    csv_dict_sub[tb_plot]['end_time'] = csv_dict_sub[tb_plot]['end_dt'].dt.time.astype(str)
    csv_dict_sub[tb_plot]['start_time'] = pd.to_datetime(csv_dict_sub[tb_plot]['start_time'])
    csv_dict_sub[tb_plot]['end_time'] = pd.to_datetime(csv_dict_sub[tb_plot]['end_time'])

    csv_dict_sub[tb_plot]['type_decoded'] = csv_dict_sub[tb_plot]['type_decoded'].replace('DATA COLLECTION STARTED', 'DEVICE OFF')
    csv_dict_sub[tb_plot]['type_decoded'] = csv_dict_sub[tb_plot]['type_decoded'].replace('INACC', 'DEVICE OFF')
    csv_dict_sub[tb_plot]['type_decoded'] = csv_dict_sub[tb_plot]['type_decoded'].replace('OFF', 'DEVICE OFF')

    csv_dict_sub[tb_plot]['subtype_decoded'] = csv_dict_sub[tb_plot]['subtype_decoded'].replace('ACTIVITY', 'OTHER ACTIVITIES')
    csv_dict_sub[tb_plot]['subtype_decoded'] = csv_dict_sub[tb_plot]['subtype_decoded'].replace('TRIP', 'OTHER TRIPS')
    # csv_dict[tb_plot]['subtype_decoded'] = csv_dict[tb_plot]['subtype_decoded'].replace('VEHICLE', 'OTHER TRIPS')

    csv_dict_sub[tb_plot]['subtype_decoded'] = csv_dict_sub[tb_plot]['subtype_decoded'].replace('WORK', 'WORKPLACE')

    csv_dict_sub[tb_plot].loc[(csv_dict_sub[tb_plot]['type_decoded']=='ACTIVITY')&(csv_dict_sub[tb_plot]['subtype_decoded']=='OTHER'), 'subtype_decoded'] = 'OTHER ACTIVITIES'
    csv_dict_sub[tb_plot].loc[(csv_dict_sub[tb_plot]['type_decoded']=='TRIP')&(csv_dict_sub[tb_plot]['subtype_decoded']=='OTHER'), 'subtype_decoded'] = 'OTHER TRIPS'
#     csv_dict_sub[tb_plot].groupby(['type_decoded', 'subtype_decoded'])['id'].agg('count')

    # 3. filter activities in valid days and save activities as a new table 'ucalitems_activity'
    tb_origin='ucalitems_ljoin_ucisurvey'
    tb_sub = 'ucalitems_activity'

    csv_dict_sub[tb_sub] = query_valid_days_func(csv_dict_origin, tb=tb_origin, query_text=query_text)
    csv_dict_sub[tb_sub] = csv_dict_sub[tb_sub].query('(type_decoded=="ACTIVITY")&(centroid==centroid)')
    
    print('# activities with centroid_cor after filtering: {0:0.0f}.'.format(csv_dict_sub[tb_sub].shape[0]))
    
    # Save filtered data (original tables with filtered hours)
    # Note: the table exit_survey is not included because it's not complete (only 9 records)
    # Note: the table ucalitems (trip / activity episodes) is not included because it contians episodes cross multiple days and can not be assigned to a single day. Instead, please use the saved table ucalitems_ljoin_ucisurvey for episode-level calculation.  
    # 1. ema_survey
    csv_dict_origin['ema_survey'].rename(columns = {'ema_survey_date': 'start_date'}, inplace=True)
    csv_dict_origin['ema_survey']['start_date'] = pd.to_datetime(csv_dict_origin['ema_survey']['start_date'])
    csv_dict_sub['ema_survey'] = query_valid_days_func(csv_dict_origin, tb='ema_survey', query_text=query_text)
    
    # 2. calendar_item_survey
    csv_dict_sub['calendar_item_survey'] = pd.merge(
        left = csv_dict_origin['calendar_item_survey'].rename(columns={'calendar_item_id': 'cal_item_id', 
                             'calendar_item_timestamp': 'start_timestamp'}), 
        right = csv_dict_sub['ucalitems_ljoin_ucisurvey_split'][['user_id', 'cal_item_id', 'start_timestamp']], 
        on = ['user_id', 'cal_item_id', 'start_timestamp'], 
        how = 'inner'
        )
    tb_calsurvey = 'calendar_item_survey'
    print('Table Name: {0}. # items before filtering: {1:0.0f}. # items after filtering: {2:0.0f}'.format(tb_calsurvey, csv_dict_origin[tb_calsurvey].shape[0], csv_dict_sub[tb_calsurvey].shape[0]))
    
    return(csv_dict_sub)

"""
INPUT: csv_dict <original data dictionary after S2 but before applying the filtering function above>,
       userids      

OUTPUT: a subset of csv_dict
"""
def select_userids(csv_dict, userids):
    
    csv_dict_sub = {}
    
    for key, value in csv_dict.items():
        csv_dict_sub[key] = value.query('user_id==@userids')
            
    return(csv_dict_sub)

if __name__=='__main__':
    pass