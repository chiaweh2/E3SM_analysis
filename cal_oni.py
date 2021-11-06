#!/usr/bin/env python
# coding: utf-8

# # Calculate the Oceanic Nino Index
# Nino3.4 index use the model output "tos" which is representing the
#  sea surface temperaturesea (SST) usually measured over the ocean.
#  Warm (red) and cold (blue) periods based on a threshold of +/- 0.5C for
#  the Oceanic Nino Index (ONI) [3 month running mean of ERSST.v5 SST anomalies
#  average over the Pacific Ocean tropic region in the Nino 3.4 region
#  (5N-5S, 120-170W)], based on centered 30-year base periods updated every 5 years.
#  (http://origin.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/ONI_v5.php)

import os
import cftime
import dask
import xarray as xr
import numpy as np
import sys


def model_oni(da_model_tos,area_weight=None,remove_30mean=True):
    
    lon_range_list = [-170+360,-120+360]    # Lon: -180-180
    lat_range_list = [-5,5]                 # Lat: -90-90


    ds_model_mlist = {}
    mean_mlist = {}
    season_mlist = {}

    var = 'tos'
    # store all model data
    ds_model_mlist[var] = da_model_tos

    # calculate mean
    mean_mlist[var] = ds_model_mlist[var].mean(dim='time').compute()
    ds_model_mlist[var] = ds_model_mlist[var]-mean_mlist[var]

    # calculate seasonality
    season_mlist[var] = ds_model_mlist[var].groupby('time.month').mean(dim='time').compute()
    ds_model_mlist[var] = ds_model_mlist[var].groupby('time.month')-season_mlist[var]


    regional_var_mlist = xr.Dataset()


    #### setting individual event year range
    lon_range  = lon_range_list
    lat_range  = lat_range_list

    # read areacello
    if area_weight is not None:
        da_area = area_weight
        da_area = da_area.where(
                              (da_area.lon>=np.min(lon_range))&
                              (da_area.lon<=np.max(lon_range))&
                              (da_area.lat>=np.min(lat_range))&
                              (da_area.lat<=np.max(lat_range))
                               ).compute()
    else :
        da_area =(mean_mlist[var]/mean_mlist[var]).where(
                              (mean_mlist[var].lon>=np.min(lon_range))&
                              (mean_mlist[var].lon<=np.max(lon_range))&
                              (mean_mlist[var].lat>=np.min(lat_range))&
                              (mean_mlist[var].lat<=np.max(lat_range))
                               ).compute()

    # calculate the temporal mean of regional mean
    mean_var = mean_mlist[var]
    regional_mean = (mean_var*da_area).sum(dim=['lon','lat'])/(da_area).sum()
    regional_mean = regional_mean.compute()

    # calculate time varying regional mean
    regional_var_mlist['oni'] = ((ds_model_mlist[var]*da_area).sum(dim=['lon','lat'])/(da_area).sum(dim=['lon','lat'])).compute()
    regional_var_mlist['oni'] = regional_var_mlist['oni']+regional_mean

    # calculate 3 month moving average
    regional_var_mlist['oni']\
        = regional_var_mlist['oni'].rolling(dim={"time":3},min_periods=3,center=True).mean()


    # removing 30 year mean for each 5 year period located at the center of the 30 year window
    if remove_30mean:
        moving_window=30                                   # years
        num_year_removemean=5                              # years
        da_oni_noclim=regional_var_mlist['oni'].copy()
        da_moving_mean=np.zeros(len(da_oni_noclim))
        for ii in range(0,len(da_oni_noclim),num_year_removemean*12):
            if ii < moving_window/2*12 :
                da_moving_mean[ii:ii+5*12]=da_oni_noclim[:ii+15*12].mean().values
            elif ii > len(da_oni_noclim)-moving_window/2*12:
                da_moving_mean[ii:ii+5*12]=da_oni_noclim[-15*12+ii:].mean().values
            else:
                da_moving_mean[ii:ii+5*12]=da_oni_noclim[ii-15*12:ii+15*12].mean().values
        regional_var_mlist['oni']=da_oni_noclim-da_moving_mean
    else:
        regional_var_mlist['oni'] = regional_var_mlist['oni']-regional_var_mlist['oni'].mean(dim='time')

    return regional_var_mlist['oni']