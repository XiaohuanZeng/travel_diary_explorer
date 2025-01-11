#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'Calculate activity space'

__author__ = 'Xiaohuan Zeng'

import math
import numpy as np
import pandas as pd

import geopandas as gpd
import polyline
from pointpats.centrography import mean_center, ellipse
from matplotlib.patches import Ellipse
from shapely.geometry import Polygon 

unit_convert = 1609.34 # global paramter to convert mile to meter

# convert the coordinates orignally stored as TEXT into polyline <datatype> in Python
def str2cor_row(centroid_str):
    if centroid_str=='None':
        return(np.nan)
    elif centroid_str!=centroid_str:
        return(np.nan)
    else: 
        return(polyline.decode(centroid_str, 5)[0])

# Row by Row, convert the coordinates orignally stored as TEXT into polyline by calling the function "str2cor_row(centroid_str)" 
def str2cor_tb(ucalitems, original_col = 'centroid', new_col = 'centroid_cor'):
    ucalitems[new_col] = ucalitems[original_col].map(lambda x: str2cor_row(x))
    return(ucalitems)


"""
INPUT:  ucalitems_activity: all activities in the user calendar items

TASKS:  coordinate transformation from lat/long to x/y in meters

OUTPUT: ucalitems_activity with pecified crs
"""

def extract_geo_info(ucalitems_activity, origin_crs, projected_crs):
    temp = ucalitems_activity.copy()
    
    # get seperate lon and lat numbers from tuple of (lat, lon)
    temp['lat'] = temp['centroid_cor'].str[0]
    temp['lon'] = temp['centroid_cor'].str[1]

    # dataframe to geodataframe
    result = gpd.GeoDataFrame(temp, geometry=gpd.points_from_xy(temp.lon, temp.lat))
    result.set_crs(epsg=origin_crs, inplace=True) # set the origin crs
    result.to_crs(epsg=projected_crs, inplace=True) # convert to the crs with meter as unit

    # get the x y in meters in the new set crs
    result['x'] = result['geometry'].x
    result['y'] = result['geometry'].y
    
    return(result)

"""
INPUT:  ucalitems_activity: activities items after coordinate transformation
        buffer_dis_meter: buffer distance in meters
        
TASKS:  get the geometry of the convext hull
        create buffer around the shape and get its area
        if the geometry is ONE point, set area as zero

OUTPUT: convex hull area
"""
def cal_convex_hull(ucalitems_activity, buffer_dis_meter, dissolve_col = ["user_id", "start_date"]):

    # dissove points to get convex hull
    result = ucalitems_activity.dissolve(dissolve_col).convex_hull.reset_index().set_geometry(0)
    result.columns = ["user_id", "start_date", 'geometry']
    result = gpd.GeoDataFrame(result, geometry='geometry')
    
    # buffer, and the area of buffer in square meter to square mile
    result['buffer'] = result['geometry'].buffer(buffer_dis_meter)
    result['area_meter'] = result['buffer'].area
    result['area_mile'] = result['area_meter'] / (unit_convert**2)
    result['len_meter'] = result['geometry'].length

    # set area as zero if geometry type is point
    result['geometry_type'] = result.geom_type
    result.loc[result['geometry_type']=='Point','area_mile'] = 0

    return(result)


"""
INPUT:  ucalitems_activity: activities items after coordinate transformation
        buffer_dis_meter: buffer distance in meters
        
TASKS:  get the geometry of the convext hull
        if the geometry is ONE point, set area as zero
        if the geometry is LINE, get the area of buffer areas along the line
        if the geometry is POLYGON, get the area directly, no buffer applies

OUTPUT: convex hull area
"""
def cal_convex_hull_line_buffer(ucalitems_activity, buffer_dis_meter, dissolve_col = ["user_id", "start_date"]):

    # dissove points to get convex hull
    result = ucalitems_activity.dissolve(dissolve_col).convex_hull.reset_index().set_geometry(0)
    result.columns = ["user_id", "start_date", 'geometry']
    result = gpd.GeoDataFrame(result, geometry='geometry')
    
    # get geometry_type, length in meter, and area in square meter
    result['geometry_type'] = result.geom_type
    result['len'] = result['geometry'].length
    result['area_meter'] = result['geometry'].area
    
    # set area as zero if geometry type is point
    result.loc[result['geometry_type']=='Point','area_meter'] = 0
    
    # set area as length * buffer_dis_meter if geometry type is linestring
    result.loc[result['geometry_type']=='LineString','area_meter'] = result['len'] * buffer_dis_meter
    
    # square meter to square mile
    result['area_mile'] = result['area_meter'] / (unit_convert**2)

    return(result)


"""
INPUT:  ucalitems_activity
        convex_hull
        buffer_dis_meter

OUTPUT: Standard Deviational Ellipse (SDE)
"""
# cal standard deviation ellipse by group, each group is one person one day
def cal_sde_group(group):
    points = group[['x', 'y']] # get points
    sx, sy, theta = ellipse(points) # cal parameters for ellipse
    theta_degree = np.degrees(theta) # convert theta to degree
    
    # turn ellipse parameters to polygon geometry
    my_ellipse = Ellipse(xy=mean_center(points), width=sx*2, height=sy*2, angle=-theta_degree) 
    vertices = my_ellipse.get_verts()  
    my_ellipse = Polygon(vertices)
    
    # save results as a dict
    result = {}
    result['geometry'] = my_ellipse
    result['sx_meter'] = sx
    result['sy_meter'] = sy
    result['theta'] = theta
    result['theta_degree'] = theta_degree
    return pd.Series(result, index=['geometry', 'sx_meter', 'sy_meter', 'theta', 'theta_degree'])


def cal_sde(ucalitems_activity, convex_hull, buffer_dis_meter, group_cols = ["user_id", "start_date"]): 

    # cal sde by group and dataframe to geo dataframe (using the same crs as input table)
    grouped = ucalitems_activity.groupby(group_cols)
    temp = grouped.apply(cal_sde_group)
    temp.reset_index(inplace=True)
    temp = gpd.GeoDataFrame(temp, geometry='geometry')
    temp.set_crs(epsg=ucalitems_activity.crs.to_epsg(), inplace=True)

    # merge with convex hull to get geometry type and length
    result = pd.merge(
        left = temp, 
        right = convex_hull[["user_id", "start_date", 'geometry_type', "len_meter"]], 
        on = group_cols, 
        how = 'inner'
    )
   
    #print(temp.shape, convex_hull.shape, result.shape)

    # if geometry type is point, set sx_meter and sy_meter as zero 
    result.loc[result['geometry_type']=='Point','sx_meter'] = 0
    result.loc[result['geometry_type']=='Point','sy_meter'] = 0

    # if geometry type is linestring, set sx_meter as the travel distance, and set sy_meter as the buffer_dis_meter
    result.loc[result['geometry_type']=='LineString','sx_meter'] = result['len_meter']
    result.loc[result['geometry_type']=='LineString','sy_meter'] = buffer_dis_meter

    # convert length and area from meter to mile
    result['sx_mile'] = result['sx_meter'] / unit_convert
    result['sy_mile'] = result['sy_meter'] / unit_convert
    result['area_mile'] = math.pi * result['sx_mile'] * result['sy_mile']
    
    return(result)

if __name__=='__main__':
    pass