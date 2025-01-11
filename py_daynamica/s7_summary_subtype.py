#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'Read data'

__author__ = 'Xiaohuan Zeng'

import os
import numpy as np
import pandas as pd

import seaborn as sns
import matplotlib.pyplot as plt
import xlsxwriter

from py_daynamica import s3_valid_data, s6_daily_episode_summary

unit_convert = 1609.344  # global paramter to convert between miles and meters

# figure configuration
# 1. color for subtype
# link to the color reference: https://colorbrewer2.org/#type=qualitative&scheme=Paired&n=12
activity_color_discrete_map = {
'HOME': '#f6e8c3',  
'WORKPLACE': '#bebada',  
'EDUCATION': '#ccebc5', 
'FOOD & MEAL': '#b3de69',
'MEDICAL & FITNESS': '#80b1d3',
'FUN & LEISURE': '#fdb462',
'COMMUNITY & CULTURAL': '#ffed6f',
'RELIGIOUS & SPIRITUAL': '#ffffb3',
'CARE GIVING': '#fb8072',
'SHOPPING ERRANDS': '#bc80bd',
'CIVIL ERRANDS': '#e31a1c',
'OTHER ACTIVITIES': '#d9d9d9', 
'TRIP': '#b15928',
'DEVICE OFF': '#f0f0f0'
}

trip_color_discrete_map = {
'CAR - DRIVER': '#fdbf6f',
'CAR - PASSENGER': '#ff7f00',
'VEHICLE': '#b15928', 
'TAXI/UBER/LYFT': '#fb9a99',
'RAIL': '#1f78b4', 
'BUS': '#a6cee3',
'BIKE': '#33a02c',
'WALK': '#b2df8a',
'WAIT': '#6a3d9a',
'OTHER TRIPS': '#d9d9d9',
}

figure_color_maps = {
'ACTIVITY': activity_color_discrete_map, 
'TRIP': trip_color_discrete_map
}

# 2. figure height, title
figure_height = {
'ACTIVITY': 4.7, 
'TRIP': 3.2
}

figure_title = {
'ACTIVITY': 'Numeric values of activity duration and frequency per person per day by activity type.', 
'TRIP': 'Numeric values of trip duration, distance and frequency per person per day by trip type.'
}

duration_unit = {
'ACTIVITY': 1, 
'TRIP': 60
}

# rename column names
col_dict_final = {
    'ACTIVITY': {'id': 'Activity Counts', 
    'duration_after_split': 'Activity Duration in Hours'}, 
    'TRIP': {'id': 'Trip Counts', 
    'duration_after_split': 'Trip Duration in Minutes', 
    'distance_after_split': 'Trip Distance in Miles'}
}

# columns to aggregate and aggregation function
agg_dict = {
    'ACTIVITY': {'id': 'count', 'duration_after_split': 'sum'}, 
    'TRIP': {'id': 'count', 'duration_after_split': 'sum', 'distance_after_split': 'sum'}
}

# get the number of valid days by all days, weekend, weekday
def get_valid_days(day_summary, trip_count=-1):
    df = day_summary.query('trip_count>@trip_count')
    valid_days_count = {}
    valid_days_count['All Days'] = df.shape[0]
    valid_days_count['Weekend'] = df.query('IsWeekend=="Weekend"').shape[0]
    valid_days_count['Weekday'] = df.query('IsWeekend=="Weekday"').shape[0]
    
    print(valid_days_count)
    return(valid_days_count)

"""
INPUT: ucalitems
    day_summary
    mytype: 'trip' or 'activity'
    trip_count: -1 to include all days, 0 to include the days with >0 trips
OUTPUT: a table to summarize the duration (and distance) by subtypes

"""
def activity_trip_subtype(ucalitems, day_summary, mytype, trip_count=-1): 
    valid_days_count = get_valid_days(day_summary, trip_count = trip_count)
    
    # select episodes of trips or activities
    df = ucalitems.query('type_decoded==@mytype')
    # print(df.shape)
    
    # convert the unit, hour for activity, minutes and miles for trips,
    df['duration_after_split'] = df['duration_after_split'].copy()*duration_unit[mytype]
    df['distance_after_split'] = df['distance_after_split'] / unit_convert 
    
    # groupby and aggregate
    df = df.groupby(['user_id', 'IsWeekend', 'start_date', 'subtype_decoded']).agg(agg_dict[mytype])
    df.reset_index(inplace=True)
    cols = list(agg_dict[mytype].keys())
    
    # calculate mean for all days of a week, weekends, and weekdays
    result = []
    for key, value in valid_days_count.items():
        df_sub = df.copy()
        if key != 'All Days':
            df_sub = df_sub.query('IsWeekend==@key')
        agg_re = (df_sub.groupby(['subtype_decoded'])[cols].agg('sum') / value).reset_index()
        agg_re = pd.melt(agg_re, id_vars=['subtype_decoded'], value_vars=cols)
        agg_re['day_type'] = key

        result.append(agg_re)
    
    # reformat the resulted table, including column names, sort rows, pivot table, ...
    result_df = pd.concat(result)
    result_df['variable'] = result_df['variable'].map(col_dict_final[mytype])
    result_df.sort_values(by=['variable', 'day_type'], inplace=True)
    type_col = ' '.join([mytype.title(), 'Type'])
    result_df.rename(columns={'subtype_decoded': type_col}, inplace=True)
    result_df = result_df.pivot(index=type_col, columns=['variable', 'day_type'], values='value').reset_index()

    # add a row to calculate the total
    result_df = result_df.append(result_df.sum(numeric_only=True), ignore_index=True)
    result_df.iloc[:, 0] = result_df.iloc[:, 0].fillna('Total')

    # sort the subtypes by the specified order
    sort_cols = list(figure_color_maps[mytype].keys())+['Total']
    sort_col = (               type_col,         '')
    result_df[sort_col] = result_df[sort_col].astype("category")
    result_df[sort_col] =  result_df[sort_col].cat.set_categories(sort_cols)
    result_df.sort_values(sort_col, inplace=True)
    result_df[sort_col] = result_df[sort_col].astype("str")
    result_df.fillna(0, inplace=True)
    
    return(result_df)


"""
INPUT:  ucalitems: can the the table before or after leg2trip
        tb: table name of ucalitems
        day_summary
        mytype: 'trip' or 'activity'
        directory: the folder to save the figure
        agg_col: id, duration or distance
        agg_func: count (id) or sum (duration or distance)

OUTPUT: a figure to summarize the duration (and distance) by subtypes

"""
def activity_trip_subtype_figure(ucalitems, tb, day_summary, mytype, directory, agg_col, agg_func): 
    valid_days_count = get_valid_days(day_summary)
    
    df = ucalitems.copy()

    # reclassify the subtype and type
    if (mytype=='ACTIVITY') & (agg_col=='duration_after_split'):
        df.loc[df['type_decoded'] == 'TRIP', 'subtype_decoded'] = 'TRIP'
        df.loc[df['type_decoded'] == 'TRIP', 'type_decoded'] = 'ACTIVITY'
        df.loc[df['type_decoded'] == 'DEVICE OFF', 'subtype_decoded'] = 'DEVICE OFF'
        df.loc[df['type_decoded'] == 'DEVICE OFF', 'type_decoded'] = 'ACTIVITY'
        df['subtype_decoded'] = df['subtype_decoded'].replace('WORK', 'WORKPLACE')
    
    df = df.query('type_decoded==@mytype')
    
    # print(df.shape)
    # convert the unit, hour for activity, minutes and miles for trips,
    df['duration_after_split'] = df['duration_after_split'].copy()*duration_unit[mytype]
    df['distance_after_split'] = df['distance_after_split'] / 1609.344 

    # groupby and aggregate
    df = df.groupby(['user_id', 'IsWeekend', 'start_date', 'subtype_decoded']).agg({agg_col: agg_func})
    df.reset_index(inplace=True)
    new_agg_col = col_dict_final[mytype][agg_col]
    df.rename(columns = {agg_col: new_agg_col}, inplace=True)
    
#     if mytype=='ACTIVITY':
#         cols = ['_'.join([mytype.title(), x]) for x in ['Counts', 'Duration']]
    
    # calculate mean for all days, weekend, and weekdays
    result = []
    for key, value in valid_days_count.items():
        df_sub = df.copy()
        if key != 'All Days':
            df_sub = df_sub.query('IsWeekend==@key')
        agg_re = (df_sub.groupby(['subtype_decoded'])[[new_agg_col]].agg('sum') / value).reset_index()
        agg_re = pd.melt(agg_re, id_vars=['subtype_decoded'], value_vars=[new_agg_col])
        agg_re['day_type'] = key

        result.append(agg_re)

    # reformat the resulted table to prepare for the plotting
    type_col = ' '.join([mytype.title(), 'Type'])
    
    result_df = pd.concat(result)
    result_df.rename(columns={'subtype_decoded': type_col}, inplace=True)
    result_df = result_df.pivot(index=['day_type'], columns=type_col, values='value')
    
    if (mytype=='ACTIVITY') & (agg_col=='duration_after_split'):
        result_df = result_df.div(result_df.sum(axis=1), axis=0) * 24
    
    # create and format the figure
    plt.rcParams.update({'font.size': 12})
    fig, ax = plt.subplots(figsize=(8,figure_height[mytype]))
    
    for col in list(figure_color_maps[mytype].keys()):
        # print(col in result_df.columns)
        if not (col in result_df.columns):
            result_df[col] = 0
    
    result_df[list(figure_color_maps[mytype].keys())].plot.barh(stacked=True, ax=ax, color=figure_color_maps[mytype])
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
    ax.set(xlabel=new_agg_col, ylabel="") #, title=figure_title[mytype]

    # save the figure
    if (tb=='ucalitems_temporal_plot') & (mytype=='TRIP'):
        fig.savefig(os.path.join(directory,'{}_segment_{}.png'.format(mytype, new_agg_col)), bbox_inches='tight', dpi=600)
    elif tb=='leg2trip':
        fig.savefig(os.path.join(directory,'{}_complete_{}.png'.format(mytype, new_agg_col)), bbox_inches='tight', dpi=600)
    else:
        fig.savefig(os.path.join(directory,'{}_{}.png'.format(mytype, new_agg_col)), bbox_inches='tight', dpi=600)
        
    return(result_df)


# different functions to format different tables
def format_valid_days(df, sheetname, workbook, writer):
    df.to_excel(writer, sheet_name=sheetname, index=False)
    worksheet = writer.sheets[sheetname]
    
    # Format all the columns.
    my_format = workbook.add_format(dict(align='center', valign='vcenter', text_wrap=True, border=1))
    worksheet.set_column(1, df.shape[1]-1, 15, my_format)
    
    # first row and column
    my_format_bold = workbook.add_format(dict(align='center', valign='vcenter', text_wrap=True, border=1, bold=True))
    worksheet.set_row(0, None, my_format_bold)
    worksheet.set_column(0, 0, 15, my_format_bold)
    
def format_daily_statistics(df, sheetname, workbook, writer):
    df.to_excel(writer, sheet_name=sheetname, index=False)
    worksheet = writer.sheets[sheetname]
    
    # Format all the columns.
    my_format = workbook.add_format(dict(align='right', valign='vcenter', text_wrap=True, border=1, num_format='0.00'))
    worksheet.set_column(1, df.shape[1]-1, 9, my_format)
    
    # first row and column
    my_format_bold = workbook.add_format(dict(align='left', valign='vcenter', text_wrap=True, border=1, bold=True))
    worksheet.set_row(0, None, my_format_bold)
    worksheet.set_column(0, 0, 36, my_format_bold)
    
def format_subtype(df, sheetname, workbook, writer):
    index_save=True
    df.to_excel(writer, sheet_name=sheetname, index=index_save)
    worksheet = writer.sheets[sheetname]
    
    # Format all the columns.
    my_format = workbook.add_format(dict(align='right', valign='vcenter', text_wrap=True, border=1, num_format='0.00'))
    worksheet.set_column(1, df.shape[1]-1+index_save, 7, my_format)
    
    # first row and column
    my_format_bold = workbook.add_format(dict(align='left', valign='vcenter', text_wrap=True, border=1, bold=True))
    worksheet.set_row(0, None, my_format_bold)
    worksheet.set_column(0+index_save, 0+index_save, 36, my_format_bold)    

# save multiple tables and plots in a batch

'''
Input: 
- directory to save results
- csv_dict before filtering (to create tbales to examine valid days)
- csv_dict_sub after filtering 

Output
- A excel with 6 sheets
- Seven figures

'''


def save_tables_plots(directory, csv_dict, csv_dict_sub):
        
    print(csv_dict['ucalitems_ljoin_ucisurvey_split'].shape, 
          csv_dict_sub['ucalitems_ljoin_ucisurvey_split'].shape)

    # summary tables
    subtype_confirmed_hours = s3_valid_data.count_valid_per_days(day_summary = csv_dict['day_summary'], 
                                                                 numerator_col = 'with_subtype', 
                                                                 denominator_filter = 'interact_by_confirm>0')
    survey_answered_hours = s3_valid_data.count_valid_per_days(day_summary = csv_dict['day_summary'], 
                                                               numerator_col = 'with_survey', 
                                                               denominator_filter = '(interact_by_confirm>0)&(with_subtype>=(12-0.1))')
    daily_summary = s6_daily_episode_summary.overview_statistics(csv_dict = csv_dict_sub, 
                                                                 stat_group_cols = ['Statistics'])
    activity = activity_trip_subtype(ucalitems=csv_dict_sub['ucalitems_temporal_plot'], 
                                         day_summary=csv_dict_sub['day_summary'], 
                                         mytype='ACTIVITY', 
                                         trip_count=-1)
    trip = activity_trip_subtype(ucalitems=csv_dict_sub['ucalitems_temporal_plot'], 
                                         day_summary=csv_dict_sub['day_summary'], 
                                         mytype='TRIP', 
                                         trip_count=-1)
    complete_trip = activity_trip_subtype(ucalitems=csv_dict_sub['leg2trip'], 
                                         day_summary=csv_dict_sub['day_summary'], 
                                         mytype='TRIP', 
                                         trip_count=-1)
    
    # write to excel and format
    writer = pd.ExcelWriter(r'{}\tables.xlsx'.format(directory), engine='xlsxwriter')
    workbook  = writer.book
    
    format_valid_days(subtype_confirmed_hours, 'subtype_confirmed_hours', workbook, writer)
    format_valid_days(survey_answered_hours, 'survey_answered_hours', workbook, writer)
    format_daily_statistics(daily_summary, 'daily_summary', workbook, writer)
    format_subtype(activity, 'activity_subtype', workbook, writer)
    format_subtype(trip, 'trip_segment_subtype', workbook, writer)
    format_subtype(complete_trip, 'trip_complete_subtype', workbook, writer)
    
    workbook.close()
    
    #figures
    activity_trip_subtype_figure(ucalitems=csv_dict_sub['ucalitems_temporal_plot'], 
                             tb = 'ucalitems_temporal_plot', 
                             day_summary=csv_dict_sub['day_summary'], 
                             mytype='ACTIVITY', 
                             directory=directory, 
                             agg_col='duration_after_split', 
                             agg_func='sum')
    

    
    activity_trip_subtype_figure(ucalitems=csv_dict_sub['ucalitems_temporal_plot'], 
                             tb = 'ucalitems_temporal_plot', 
                             day_summary=csv_dict_sub['day_summary'], 
                             mytype='TRIP', 
                             directory=directory, 
                             agg_col='id', 
                             agg_func='count')
    
    activity_trip_subtype_figure(ucalitems=csv_dict_sub['ucalitems_temporal_plot'], 
                             tb = 'ucalitems_temporal_plot', 
                             day_summary=csv_dict_sub['day_summary'], 
                             mytype='TRIP', 
                             directory=directory, 
                             agg_col='duration_after_split', 
                             agg_func='sum')
    
    activity_trip_subtype_figure(ucalitems=csv_dict_sub['ucalitems_temporal_plot'], 
                             tb = 'ucalitems_temporal_plot', 
                             day_summary=csv_dict_sub['day_summary'], 
                             mytype='TRIP', 
                             directory=directory, 
                             agg_col='distance_after_split', 
                             agg_func='sum')
    
    activity_trip_subtype_figure(ucalitems=csv_dict_sub['leg2trip'], 
                             tb = 'leg2trip', 
                             day_summary=csv_dict_sub['day_summary'], 
                             mytype='TRIP', 
                             directory=directory, 
                             agg_col='id', 
                             agg_func='count')
    
    activity_trip_subtype_figure(ucalitems=csv_dict_sub['leg2trip'], 
                             tb = 'leg2trip', 
                             day_summary=csv_dict_sub['day_summary'], 
                             mytype='TRIP', 
                             directory=directory, 
                             agg_col='duration_after_split', 
                             agg_func='sum')
    
    activity_trip_subtype_figure(ucalitems=csv_dict_sub['leg2trip'], 
                             tb = 'leg2trip', 
                             day_summary=csv_dict_sub['day_summary'], 
                             mytype='TRIP', 
                             directory=directory, 
                             agg_col='distance_after_split', 
                             agg_func='sum')
    

# PresonDay Summary
# Each subtype is a column
# For each subtype for trip leg (count, distance_meter, duration_minute)
# For each subtype for whole trip (count, distance_meter, duration_minute)
# For each subtype for activity (count, duration_minute)

def person_day_subtype(ucalitems, day_summary, mytype): 
    
    # select episodes of trips or activities
    df = ucalitems.query('type_decoded==@mytype')
    # print(df.shape)
    
    # convert the unit, hour for activity, minutes and miles for trips,
    df['duration_after_split'] = df['duration_after_split'].copy()*duration_unit[mytype]
    df['distance_after_split'] = df['distance_after_split'] / unit_convert 
    
    # groupby and aggregate
    df = df.groupby(['user_id', 'IsWeekend', 'start_date', 'subtype_decoded']).agg(agg_dict[mytype])
    df.reset_index(inplace=True)
    df.rename(columns = col_dict_final[mytype], inplace=True)
    
    # pivot long to wide tables
    cols = list(col_dict_final[mytype].values())
    result_df = df.pivot(index=['user_id', 'IsWeekend', 'start_date'], columns=['subtype_decoded'], values=cols).reset_index()
    renamed_columns = []
    for i in result_df.columns:
        if i[1] == '':
            renamed_columns.append(i[0])
        else:
            renamed_columns.append('_'.join(i))
    result_df.columns = renamed_columns
    result_df.fillna(0, inplace=True)
    
    return(result_df)    
    
if __name__=='__main__':
    pass