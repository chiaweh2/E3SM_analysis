#!/usr/bin/env python
# coding: utf-8

# # E3SM IO
# This script covert the scattered simulation result into variable based single file


from dask.distributed import Client
from io_model import E3SM_coare_daily_cori_io_dask
import xarray as xr


client = Client(n_workers=1,threads_per_worker=100,processes=False)


histlist = ['h1','h2','h3','h4']

varlist_h1 = ['TS', 'TSMN', 'TSMX', 'U10', 'U200', 'U850', 'V850', 'QBOT', 'PRECC',
              'PRECL', 'SST', 'PSL', 'PS', 'QREFHT', 'QRL', 'QRS', 'TAUX', 'TAUY',
              'TREFHT', 'PBLH']

varlist_h2 = ['FLDS', 'FLNS', 'FLNSC', 'FLNT', 'FLUT', 'FLUTC', 'FSDS', 'FSDSC',
              'FSNS', 'FSNSC', 'FSNT', 'FSNTC', 'FSNTOA', 'FSNTOAC', 'FSUTOA',
              'LHFLX', 'SHFLX', 'PHIS', 'PRECC', 'PRECL', 'TMQ', 'QFLX']

varlist_h3 = ['T', 'Q', 'OMEGA', 'U', 'V', 'Z3']

varlist_h4 = ['LANDFRAC', 'OCNFRAC', 'CLDICE', 'CLDTOT', 'PTEQ', 'PTTEND', 'DCQ',
              'DTCOND', 'TTEND_TOT']

varlist = [varlist_h1,varlist_h2,varlist_h3,varlist_h4]



output_dir = '/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/20211029_mod_coare30.HIST2000_branched_all.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl/'

for nhist,hist in enumerate(histlist):
    print("Collecting %s data"%hist)
    ds_e3sm_h = E3SM_coare_daily_cori_io_dask(hist=hist,case='coare30')
    for var in varlist[nhist]:
        print("===============================")
        print('Output %s'%var)
        print('Located in %s'%output_dir)
        ds_e3sm_h[var].to_netcdf(output_dir+'HIST2000_branched_atmos_%s.nc'%var)
