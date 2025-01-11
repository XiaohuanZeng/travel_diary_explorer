#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'Plot individual temporal sequences'

__author__ = 'Xiaohuan Zeng'

import os
import pandas as pd
import plotly.express as px

# configurations for plots

# 1. colors for activity and trip types
## Use ColorBrewer Qualitative 12 classes as the reference 
## https://colorbrewer2.org/#type=qualitative&scheme=Paired&n=12

color_discrete_map = {
# use beige color for home similar as setting background using beige census blocks
'HOME': '#f6e8c3',  # try also '#f6e8c3' '#fff7bc'

# use light colors for work or education as they may last long hours
'WORKPLACE': '#bebada',  # light teal
'EDUCATION': '#ccebc5', # light levendar ccebc5

# use medium dark, green/blue color for them - healthy, frequent but short
'FOOD & MEAL': '#b3de69',
'MEDICAL & FITNESS': '#80b1d3',

# use medium dark, yellow color for them - happy, less frequent
'FUN & LEISURE': '#fdb462',
'COMMUNITY & CULTURAL': '#ffed6f',
'RELIGIOUS & SPIRITUAL': '#ffffb3',

# use medium dark, warm color for them - highlight supporting activities
'CARE GIVING': '#fb8072',
'SHOPPING ERRANDS': '#bc80bd',
'CIVIL ERRANDS': '#e31a1c',

# combine these two as OTHERS and use grey for them
# 'OTHER': '#d9d9d9',
# 'ACTIVITY': '#d9d9d9',
'OTHER ACTIVITIES': '#d9d9d9',

### -------------- darker colors for trips --------------
# car is most frequent, so use medium dark beige colors similar to home's color
'CAR - DRIVER': '#fdbf6f',
'CAR - PASSENGER': '#ff7f00',
'VEHICLE': '#b15928', 

# shared mobility, try light purple or pink
'TAXI/UBER/LYFT': '#fb9a99', # or '#fb9a99' '#cab2d6'

# public transit, should be highlighted, use dark blue and green 
'RAIL': '#1f78b4', 
'BUS': '#a6cee3',

# active travel mode, also should be highlighted, use dark purpole and red
'BIKE': '#33a02c',
'WALK': '#b2df8a',

# wait may be quite short, so use dark brown may be good 
'WAIT': '#6a3d9a',

# consider combine these three as OTHERS and use grey for them similar to activities
'OTHER TRIPS': '#d9d9d9',

# missing data could be white or grey, lets discuss it 
'UNKNOWN': 'white'}

# 2. height of rectangulars in the temporal plots
heigh_dict = {'ACTIVITY':.65, 'TRIP':.85, 'DEVICE OFF': .65}

# 3. pattern in the plots - trips are shaded polygons ///
pattern_shape_map = {'ACTIVITY': '', 'TRIP': '/', 'DEVICE OFF': ''}


# configuration for html
config = {
  'toImageButtonOptions': {
    'format': 'png', # select png, svg, jpeg, OR webp
    # 'filename': 'custom_image',
    # 'height': 500,
    # 'width': 700,
    'scale':6 # Multiply title/legend/axis/canvas sizes by this factor
  }
}


"""
INPUT:  ucalitems_temporal_plot after filtering valid days, 
        user_id: user id, commonly the email address
        directory/folder to save the plot
        
TASKS:  create plots to show activity-trip sequences of a participant
        the activity and trip will have different heights
        the color of the bar indicate different activity types or travel modes
    
OUTPUT: plots for each individual person saved as images  (html files)
"""
def plot_indi_temp(ucalitems_temporal_plot, user_id, directory, 
               color_discrete_map = color_discrete_map, 
               pattern_shape_map = pattern_shape_map, 
               config = config):
    
    # select the valid episodes for the user and sort by time
    df = ucalitems_temporal_plot.query('user_id==@user_id')
    df.sort_values(by='start_dt', inplace=True, ignore_index=True)

    # check if the the number of selected episodes > 0
    if df.shape[0]>0: 
        
        # format the date string to show on the plots
        df['Date'] = df['start_date'].astype(str) + '<br>' + df['dow'].astype(str)
        df.sort_values(by='Date', inplace=True, ignore_index=True)

        # create timeline plot
        fig = px.timeline(df, x_start="start_time", x_end="end_time", y="Date",
                          pattern_shape='type_decoded', color="subtype_decoded", 
                          category_orders =  {'type_decoded': ['ACTIVITY', 'TRIP', 'DEVICE OFF'], 'subtype_decoded': list(color_discrete_map.keys())}, 
                          color_discrete_map = color_discrete_map, 
                          pattern_shape_map = pattern_shape_map
                         
                         )
        fig.update_yaxes(categoryorder="category descending") # otherwise tasks are listed from the bottom up

        # update rectangular weight and legend labels
        for i, d in enumerate(fig.data):
            mykey = d.name.split(', ')[1]

            d.width = heigh_dict[mykey]
            d.name = d.name.split(', ')[0]
            d.legendgroup = mykey
            d.legendgrouptitle= {'text': mykey}
        
        # update background color, legend orientation and xaxis tickformat
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", 
                          legend= dict(title = {'text': ''}, orientation = 'v'), 
                          xaxis=dict(tickformat="%H:%M", title='Time'), 
                            autosize=False,
                            width=1000,
                            height=300+50*df['Date'].unique().shape[0], 
                          font=dict(
                            family="Calibri",
                            size=18,  # Set the font size here
                            color="#000000"
                        )
                         )

        # save plot as html file
        fig.write_html(os.path.join(directory, "{}.html".format(user_id)), config=config) 
        print(user_id, df.shape[0])

if __name__=='__main__':
    pass