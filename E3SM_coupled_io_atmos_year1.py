#!/usr/bin/env python
# coding: utf-8

# # E3SM IO
# This script covert the scattered simulation result into variable based single file


from dask.distributed import Client
from io_model import E3SM_daily_year1_cori_io_dask
import xarray as xr


client = Client(n_workers=1,threads_per_worker=100,processes=False)


histlist = ['h1','h2','h3']

varlist_h1 = ['PRECT','PS','PSL','QBOT','QREFHT','QRL','QRS','SST','TAUX','TAUY',  
              'TREFHT','TS','U10','U200','U850','V850']

varlist_h2 = ['CLDTOT','DCQ','DTCOND','FLDS','FLNS','FLNT','FLUT','FSNS','FSNT',
              'LHFLX','PHIS','PRECC','PRECL','SHFLX','TMQ']

varlist_h3 = ['OMEGA','Q','T','U','V','Z3']


varlist = [varlist_h1,varlist_h2,varlist_h3]


output_dir = '/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/20210501.HIST2000_branched_all.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl/'

for nhist,hist in enumerate(histlist):
    print("Collecting %s data"%hist)
    ds_e3sm_h = E3SM_daily_year1_cori_io_dask(hist=hist,realm='atm')
    for var in varlist[nhist]:
        print("===============================")
        print('Output %s'%var)
        print('Located in %s'%output_dir)
        ds_e3sm_h[var].to_netcdf(output_dir+'HIST2000_branched_atmos_%s_year1.nc'%var)
