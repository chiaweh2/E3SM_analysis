from ocean_mask import levitus98
from memory import used_memory
import xarray as xr
import numpy as np
import xesmf as xe
import os
import warnings
import datetime
import dask



def E3SM_daily_cori_io(lon_lim,lat_lim,case=None):

    """
    The model io for the rerun on Cori. 
    If there are more needed model variables, a manual 
    added variable in the setting section is available.
    This is E3SM daily output from DOE funded project on E3SM.

    The io function also calculate the 
        1. surface saturated specific humidity (qsurf)
        2. specific humidity difference between qsurf and huss (del_q)

    To accomplish the calculation of qsurf
        - ts (surface temperature)
        - psl (sea level pressure)
    are nesessary variables in this model io. 

    Inputs:
    =================
    lon_lim (list) : a list object containing the min and max value of 
                     longitude of the desired region.

    lat_lim (list) : a list object containing the min and max value of 
                     latitude of the desired region.
    
    case (string) : option including 'UA', 'CTL', and 'COARE'. Default is None
                    need to pick sepcific string for the function to work.


    Outputs:
    =================
    ds_all_ts (xr.Dataset) : a xarray dataset in the form of dask array 

    """

    if case in ['365test'] :
        casename = '20210501.HIST2000_365days.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
    elif case in ['2day_cont']:
        casename = '20210501.HIST2000_2days_test_cont.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
    else :
        print('Please enter casename')
    #     return

    # file name setting
    data_h1_vars = ['PS',
                    'TS',
                    'QBOT',
                    'U10']
    data_h2_vars = ['LHFLX',
                    'PRECC',
                    'PRECL']

    data_path = os.path.join('/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/',
                                                casename,
                                                'regrid')
    data_h1_files = '%s.cam.h1.????-??-??-00000.nc'%(casename)
    data_h2_files = '%s.cam.h2.????-??-??-00000.nc'%(casename)

    ds_atm_h1 = xr.open_mfdataset(os.path.join(data_path,data_h1_files),
                                         chunks={'time':100,'lat':100,'lon':100})
    ds_atm_h2 = xr.open_mfdataset(os.path.join(data_path,data_h2_files),
                                         chunks={'time':100,'lat':100,'lon':100})    


    da_list1 = [ds_atm_h1[var] for var in data_h1_vars]
    da_list2 = [ds_atm_h2[var] for var in data_h2_vars]
    da_list = da_list1+da_list2
    ds_atm = xr.merge(da_list)
    data_vars = data_h1_vars+data_h2_vars

#     # import Pacific Basin
#     da_pacific = levitus98(ds_atm,
#                            basin=['pac'],
#                            reuse_weights=False, 
#                            newvar=True, 
#                            lon_name='lon',
#                            lat_name='lat', 
#                            new_regridder_name='')


    dask.config.set({"array.slicing.split_large_chunks": False})

    ds_atm_trop = xr.Dataset()
    ds_atm_trop_daily = xr.Dataset()
    for nvar,var in enumerate(data_vars):
        ds_atm_trop[var] = (ds_atm[var])\
                                        .where((ds_atm[var].lon>=np.array(lon_lim).min())&
                                               (ds_atm[var].lon<=np.array(lon_lim).max())&
                                               (ds_atm[var].lat>=np.array(lat_lim).min())&
                                               (ds_atm[var].lat<=np.array(lat_lim).max()),
                                               drop=True)
        ds_atm_trop_daily[var] = ds_atm_trop[var].resample(time="1D").mean()


    ds_atm_trop_daily['PRECT'] = ds_atm_trop_daily['PRECC']+ds_atm_trop_daily['PRECL']
    ds_atm_trop_daily = ds_atm_trop_daily.rename({'U10':'sfcWind','LHFLX':'hfls','PRECT':'pr'})


    ds_all_ts = ds_atm_trop_daily


    # # Calculate $\Delta$q
    #
    # $\delta$ q is the specific humidity difference between
    # saturation q near surface determined by SST and 2m(3m) q.
    # - To calculate the q near surface, I use the slp and dewpoint
    #    assuming the same as SST.
    # - To calculate the q at 2m, I use the RH at 2m, T at 1m, and
    #    slp to determine the mix ratio and then the specific humidity


    import metpy.calc
    from metpy.units import units
    da_q_surf = ds_all_ts['hfls'].copy()*np.nan
    da_q_2m = ds_all_ts['hfls'].copy()*np.nan

    mixing_ratio_surf = metpy.calc.saturation_mixing_ratio(ds_all_ts['PS'].values*units.Pa,
                                                           ds_all_ts['TS'].values*units.K)
    q_surf = metpy.calc.specific_humidity_from_mixing_ratio(mixing_ratio_surf)
    q_2m = metpy.calc.specific_humidity_from_mixing_ratio(ds_all_ts['QBOT'].values)

    # unit for mixing ratio and specific humidity is kg/kg
    da_q_surf.values = q_surf.magnitude
    da_q_2m.values = q_2m

    ds_all_ts['qsurf'] = da_q_surf
    ds_all_ts['huss'] = da_q_2m
    ds_all_ts['del_q'] = ds_all_ts['qsurf']-ds_all_ts['huss']
#     ds_all_ts['del_q'] = ds_all_ts['del_q'].where(ds_all_ts['del_q']>0.)


    return ds_all_ts


def E3SM_daily_cori_io_sst(lon_lim,lat_lim,case=None):

    """
    The model io for the rerun on Cori. 
    If there are more needed model variables, a manual 
    added variable in the setting section is available.
    This is E3SM daily output from DOE funded project on E3SM.

    Inputs:
    =================
    lon_lim (list) : a list object containing the min and max value of 
                     longitude of the desired region.

    lat_lim (list) : a list object containing the min and max value of 
                     latitude of the desired region.
    
    case (string) : option including 'UA', 'CTL', and 'COARE'. Default is None
                    need to pick sepcific string for the function to work.


    Outputs:
    =================
    ds_all_ts (xr.Dataset) : a xarray dataset in the form of dask array 

    """
    
    

    if case in ['5years']:
        casename1 = '20210501.HIST2000_365days.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename2 = '20210501.HIST2000_365_cont.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename3 = '20210501.HIST2000_365_cont2.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        totalcase = [casename1,casename2,casename3]
    elif case in ['10years']:
        casename1 = '20210501.HIST2000_365days.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename2 = '20210501.HIST2000_365_cont.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename3 = '20210501.HIST2000_365_cont2.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename4 = '20210501.HIST2000_365_cont3.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename5 = '20210501.HIST2000_365_cont4.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        totalcase = [casename1,casename2,casename3,casename4,casename5]
    else :
        print('Please enter casename')
        
    # file name setting
    data_h1_vars = ['SST']

    data_vars = data_h1_vars
    da_list = []
    for casename in totalcase:
        data_path = os.path.join('/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/',
                                                casename,
                                                'regrid')

        data_h1_files = '%s.cam.h1.????-??-??-00000.nc'%(casename)

        print(os.path.join(data_path,data_h1_files))
        ds_atm_h1 = xr.open_mfdataset(os.path.join(data_path,data_h1_files),
                                         chunks={'time':100,'lat':-1,'lon':-1})


        da_list1 = [ds_atm_h1[var] for var in data_h1_vars]
        da_list = da_list+da_list1

    ds_atm = xr.concat(da_list,dim='time')   # DataArray

#         dask.config.set({"array.slicing.split_large_chunks": False})

    ds_atm_trop = xr.Dataset()
    for nvar,var in enumerate(data_vars):
        ds_atm_trop[var] = (ds_atm)\
                            .sel(lon=slice(np.array(lon_lim).min(),np.array(lon_lim).max()))\
                            .sel(lat=slice(np.array(lat_lim).min(),np.array(lat_lim).max()))


    return ds_atm_trop

def E3SM_daily_cori_io_dask(hist='h2',case='10years',realm='atm'):

    """
    The model io for the rerun on Cori. 
    If there are more needed model variables, a manual 
    added variable in the setting section is available.
    This is E3SM daily output from DOE funded project on E3SM.

    Inputs:
    =================
    lon_lim (list) : a list object containing the min and max value of 
                     longitude of the desired region.

    lat_lim (list) : a list object containing the min and max value of 
                     latitude of the desired region.
    
    case (string) : option including 'UA', 'CTL', and 'COARE'. Default is None
                    need to pick sepcific string for the function to work.


    Outputs:
    =================
    ds_all_ts (xr.Dataset) : a xarray dataset in the form of dask array 

    """
    
    

    if case in ['5years']:
        casename1 = '20210501.HIST2000_365days.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename2 = '20210501.HIST2000_365_cont.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename3 = '20210501.HIST2000_365_cont2.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        totalcase = [casename1,casename2,casename3]
    elif case in ['11years']:
        casename1 = '20210501.HIST2000_365days.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename2 = '20210501.HIST2000_365_cont.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename3 = '20210501.HIST2000_365_cont2.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename4 = '20210501.HIST2000_365_cont3.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename5 = '20210501.HIST2000_365_cont4.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        totalcase = [casename1,casename2,casename3,casename4,casename5]
    elif case in ['10years']:
        casename2 = '20210501.HIST2000_365_cont.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename3 = '20210501.HIST2000_365_cont2.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename4 = '20210501.HIST2000_365_cont3.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        casename5 = '20210501.HIST2000_365_cont4.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        totalcase = [casename2,casename3,casename4,casename5]
    else :
        print('Please enter casename')
        
    
    if realm == 'atm':
        da_list = []
        for casename in totalcase:
            data_path = os.path.join('/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/',
                                                    casename,
                                                    'regrid')

            data_hist_files = '%s.cam.%s.????-??-??-00000.nc'%(casename,hist)

            ds_atm_hist = xr.open_mfdataset(os.path.join(data_path,data_hist_files),
                                             chunks={'time':100,'lat':-1,'lon':-1})
            da_list.append(ds_atm_hist)

        ds_atm = xr.concat(da_list,dim='time')   # Dataset
        ds_out = ds_atm
        
    elif realm == 'ocn':
        da_list = []
        for casename in totalcase:
            data_path = os.path.join('/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/',
                                                    casename,
                                                    'regrid')

            data_hist_files = 'mpaso.hist.%s.????-??-??.nc'%(hist)
            ds_ocn_hist = xr.open_mfdataset(os.path.join(data_path,data_hist_files),concat_dim='Time', combine='nested')
            da_list.append(ds_ocn_hist)

        ds_ocn = xr.concat(da_list,dim='Time')   # Dataset
        ds_out = ds_ocn
        


    return ds_out


def E3SM_coare_daily_cori_io_dask(hist='h2',case='coare30',realm='atm'):

    """
    The model io for the rerun on Cori. 
    If there are more needed model variables, a manual 
    added variable in the setting section is available.
    This is E3SM daily output from DOE funded project on E3SM.

    Inputs:
    =================
    lon_lim (list) : a list object containing the min and max value of 
                     longitude of the desired region.

    lat_lim (list) : a list object containing the min and max value of 
                     latitude of the desired region.
    
    case (string) : option including 'UA', 'CTL', and 'COARE'. Default is None
                    need to pick sepcific string for the function to work.


    Outputs:
    =================
    ds_all_ts (xr.Dataset) : a xarray dataset in the form of dask array 

    """
    
    

    if case in ['coare30']:
        casename1 = '20211029_mod_coare30.HIST2000.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        totalcase = [casename1]
    elif case in ['coare35']:
        casename1 = '20211029_mod_coare35.HIST2000.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
        totalcase = [casename1]
    else :
        print('Please enter casename')
        
    
    if realm == 'atm':
        da_list = []
        for casename in totalcase:
            data_path = os.path.join('/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/',
                                                    casename,
                                                    'regrid')

            data_hist_files = '%s.cam.%s.????-??-??-00000.nc'%(casename,hist)

            ds_atm_hist = xr.open_mfdataset(os.path.join(data_path,data_hist_files),
                                             chunks={'time':100,'lat':-1,'lon':-1})
            da_list.append(ds_atm_hist)

        ds_atm = xr.concat(da_list,dim='time')   # Dataset
        ds_out = ds_atm
        
    elif realm == 'ocn':
        da_list = []
        for casename in totalcase:
            data_path = os.path.join('/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/',
                                                    casename,
                                                    'regrid')

            data_hist_files = 'mpaso.hist.%s.????-??-??.nc'%(hist)
            ds_ocn_hist = xr.open_mfdataset(os.path.join(data_path,data_hist_files),concat_dim='Time', combine='nested')
            da_list.append(ds_ocn_hist)

        ds_ocn = xr.concat(da_list,dim='Time')   # Dataset
        ds_out = ds_ocn
        


    return ds_out

def E3SM_COARE_io(lon_lim,lat_lim,case=None):

    """
    The model io for latent heat flux corrections. 
    If there are more needed model variables, a manual 
    added variable in the setting section is available.

    This is E3SM daily output from the 2000 restart on Cori.

    The io function also calculate the 
        1. surface saturated specific humidity (qsurf)
        2. specific humidity difference between qsurf and huss (del_q)

    To accomplish the calculation of qsurf
        - ts (surface temperature)
        - psl (sea level pressure)
    are nesessary variables in this model io. 

    Inputs:
    =================
    lon_lim (list) : a list object containing the min and max value of 
                     longitude of the desired region.

    lat_lim (list) : a list object containing the min and max value of 
                     latitude of the desired region.
    
    case (string) : option including 'coare30', 'coare35'. Default is None
                    need to pick sepcific string for the function to work.


    Outputs:
    =================
    ds_all_ts (xr.Dataset) : a xarray dataset in the form of dask array 

    """

    if case in ['coare30'] :
        casename = '20211029_mod_coare30.HIST2000_branched_all.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
    elif case in ['coare35']:
        casename = '20211029_mod_coare35.HIST2000_branched_all.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl'
    else :
        print('Please enter casename')
    #     return

    # file name setting
    data_vars = ['PS',
                 'TS',
                 'LHFLX',
                 'PRECC',
                 'PRECL',
                 'QBOT',
                 'U10',
                 'SST',
                 'TREFHT',
                 'QREFHT']

    data_path = os.path.join('/maloney-scratch/joedhsu/proj1/data/E3SM_simulation/',
                            casename)
    da_list = []
    for var in data_vars:
        da_list.append(xr.open_dataset(os.path.join(data_path,"HIST2000_branched_atmos_%s.nc"%var)))
   
    ds_atm = xr.merge(da_list)


    ds_atm_trop_daily = xr.Dataset()
    for nvar,var in enumerate(data_vars):
        ds_atm_trop_daily[var] = (ds_atm[var])\
                                        .where((ds_atm[var].lon>=np.array(lon_lim).min())&
                                               (ds_atm[var].lon<=np.array(lon_lim).max())&
                                               (ds_atm[var].lat>=np.array(lat_lim).min())&
                                               (ds_atm[var].lat<=np.array(lat_lim).max()),
                                               drop=True)


    ds_atm_trop_daily['PRECT'] = ds_atm_trop_daily['PRECC']+ds_atm_trop_daily['PRECL']
    ds_atm_trop_daily = ds_atm_trop_daily.rename({'U10':'sfcWind','LHFLX':'hfls','PRECT':'pr'})


    ds_all_ts = ds_atm_trop_daily


    # # Calculate $\Delta$q
    #
    # $\delta$ q is the specific humidity difference between
    # saturation q near surface determined by SST and 2m(3m) q.
    # - To calculate the q near surface, I use the slp and dewpoint
    #    assuming the same as SST.
    # - To calculate the q at 2m, I use the RH at 2m, T at 1m, and
    #    slp to determine the mix ratio and then the specific humidity


    import metpy.calc
    from metpy.units import units
    da_q_surf = ds_all_ts['hfls'].copy()*np.nan
    da_q_2m = ds_all_ts['hfls'].copy()*np.nan

    mixing_ratio_surf = metpy.calc.saturation_mixing_ratio(ds_all_ts['PS'].values*units.Pa,
                                                           ds_all_ts['TS'].values*units.K)
    q_surf = metpy.calc.specific_humidity_from_mixing_ratio(mixing_ratio_surf)
    q_2m = metpy.calc.specific_humidity_from_mixing_ratio(ds_all_ts['QREFHT'].values)

    # unit for mixing ratio and specific humidity is kg/kg
    da_q_surf.values = q_surf.magnitude
    da_q_2m.values = q_2m

    ds_all_ts['qsurf'] = da_q_surf
    ds_all_ts['huss'] = da_q_2m
    ds_all_ts['del_q'] = 0.98*ds_all_ts['qsurf']-ds_all_ts['huss']
#     ds_all_ts['del_q'] = ds_all_ts['del_q'].where(ds_all_ts['del_q']>0.)
#     ds_all_ts['SST-0.2'] = ds_all_ts['SST']-0.2
    ds_all_ts['dT'] = ds_all_ts['TS']-ds_all_ts['TREFHT']


    return ds_all_ts