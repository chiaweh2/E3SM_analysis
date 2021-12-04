#!/usr/bin/env python
# coding: utf-8

# # E3SM IO for ocean model O-MPAS output
# This script convert the scattered simulation result into variable based single file


from dask.distributed import Client, LocalCluster
import xarray as xr
import numpy as np
import os
import warnings
from io_model import E3SM_daily_year1_cori_io_dask

client = Client(n_workers=1,threads_per_worker=100,processes=False)


warnings.simplefilter("ignore")

histlist = ['am.highFrequencyOutput']

varlist_2 = ['dThreshMLD', 'pressureAdjustedSSH', 'salinityAtSurface', 'ssh',
             'tThreshMLD', 'temperatureAtSurface', 'boundaryLayerDepth']


varlist = [varlist_2]

# histlist = ['forcing_daily','am.highFrequencyOutput']

# varlist_1 = [ "windStressZonal", "windStressMeridional", "latentHeatFlux",
#              "sensibleHeatFlux", "longWaveHeatFluxUp", "longWaveHeatFluxDown",
#              "shortWaveHeatFlux", "evaporationFlux", "rainFlux", "riverRunoffFlux"]

# varlist_2 = ['dThreshMLD', 'pressureAdjustedSSH', 'salinityAtSurface', 'ssh',
#              'tThreshMLD', 'temperatureAtSurface', 'boundaryLayerDepth']


# varlist = [varlist_1,varlist_2]


output_dir = '/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/20210501.HIST2000_branched_all.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl/'

for nhist,hist in enumerate(histlist):
    print("Collecting %s data"%hist)
    ds_e3sm_h = E3SM_daily_year1_cori_io_dask(hist=hist,realm='ocn')
    for var in varlist[nhist]:
        print('output %s'%var)
        print('Located in %s'%output_dir)
        ds_e3sm_h[var].to_netcdf(output_dir+'HIST2000_branched_mpaso_%s_year1.nc'%var)
