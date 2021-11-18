#!/usr/bin/env python
# coding: utf-8

# # E3SM IO for ocean model O-MPAS output
# This script convert the scattered simulation result into variable based single file


from dask.distributed import Client
import xarray as xr
import numpy as np
import os
import warnings
from io_model import E3SM_coare_daily_cori_io_dask

client = Client(n_workers=1,threads_per_worker=10,processes=False)


warnings.simplefilter("ignore")

histlist = ['forcing_daily','diagnostic_daily']

varlist_2 = [ "windStressZonal", "windStressMeridional", "latentHeatFlux",
             "sensibleHeatFlux", "longWaveHeatFluxUp", "longWaveHeatFluxDown",
             "shortWaveHeatFlux", "evaporationFlux", "rainFlux", "riverRunoffFlux"]

varlist_3 = ["tThreshMLD", "dThreshMLD", "dGradMLD", "tGradMLD", "ssh",
             "pressureAdjustedSSH", "boundaryLayerDepth", "salinityAtSurface",
             "temperatureAtSurface", "surfaceVelocityMeridional","surfaceVelocityZonal"]


varlist = [varlist_2,varlist_3]


output_dir = '/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/20211029_mod_coare35.HIST2000_branched_all.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl/'

for nhist,hist in enumerate(histlist):
    print("Collecting %s data"%hist)
    ds_e3sm_h = E3SM_coare_daily_cori_io_dask(hist=hist,case='coare35',realm='ocn')
    for var in varlist[nhist]:
        print("===============================")
        print('output %s'%var)
        print('Located in %s'%output_dir)
        ds_e3sm_h[var].to_netcdf(output_dir+'HIST2000_branched_mpaso_%s.nc'%var)
