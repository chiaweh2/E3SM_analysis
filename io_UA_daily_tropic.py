from dask.distributed import Client, LocalCluster
import xarray as xr
import numpy as np
from io_model import UA_daily_io


client = Client(n_workers=10,threads_per_worker=100,processes=True)

# 0-360
lon_lim = [0,360]
lat_lim = [-20,20]


ds_e3sm = UA_daily_io(lon_lim,lat_lim,case='CTL').compute()
ds_e3sm['del_q'] = ds_e3sm['del_q']*1e3

ds_e3sm_coare = UA_daily_io(lon_lim,lat_lim,case='COARE').compute()
ds_e3sm_coare['del_q'] = ds_e3sm_coare['del_q']*1e3


ds_e3sm.to_netcdf('/maloney-scratch/joedhsu/proj1/data/jeyre_resample/e3sm_ctl.nc')
ds_e3sm_coare.to_netcdf('/maloney-scratch/joedhsu/proj1/data/jeyre_resample/e3sm_coare.nc')