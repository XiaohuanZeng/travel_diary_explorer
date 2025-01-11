#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'Read and save data'

__author__ = 'Xiaohuan Zeng'

import os
import pandas as pd

"""
INPUT: folder_path: the path of folder that saves daynamica data, project_name, year

OUTPUT: a dictionary (Python data type) of tables with table names modified from the original full names 

"""

def path2dict(folder_path, project_name, year):
    csv_dict = {}
    
    try:
        os.path.exists(folder_path)
        print("{} folder exists...".format(folder_path))
        print("now reading data into dictionary...")
        
        for filename in os.listdir(folder_path):
            filename_path = os.path.join(folder_path, filename)
            dict_name = filename.replace(project_name, '').replace('.csv', '').split(year)[0]
            csv_dict[dict_name] = pd.read_csv(filename_path)
            print('Tabel name: {}. # rows: {}. # columns: {} ...'.format(dict_name, str(csv_dict[dict_name].shape[0]), str(csv_dict[dict_name].shape[1])))
    
    # if folder does not exist
    except FileNotFoundError:
        print("{} folder does not exist, please check your folder path...".format(folder_path))

    return(csv_dict)

"""
INPUT:a dictionary (Python data type) of tables with table names modified from the original full names  
    folder_path: the path of folder to saves processed data
    index: True or False to keep the row index
OUTPUT: None

"""
def dict2file(csv_dict, folder_path, index = False):
    try:
        os.path.exists(folder_path)
        print("{} folder exists...".format(folder_path))
        print("now saving data into dictionary...")
        
        for filename, df_table in csv_dict.items():
            filename_path = os.path.join(folder_path, filename + '.csv')
            # remove geometry column to avoid output errors
            if filename == 'convex_hull':
                pd.DataFrame(df_table).drop(columns=['geometry', 'buffer']).to_csv(filename_path, index = index)
            elif filename == 'sde':
                pd.DataFrame(df_table).drop(columns='geometry').to_csv(filename_path, index = index)
            else: 
                df_table.to_csv(filename_path, index = index)
            print('Tabel name: {}'.format(filename,))
#             print('Tabel name: {}. # rows: {}. # columns: {} ...'.format(filename, str(df_table.shape[0]), str(df_table.shape[1])))
    
    # if folder does not exist
    except FileNotFoundError:
        print("{} folder does not exist, please check your folder path...".format(folder_path))

    return(None)

if __name__=='__main__':
    pass