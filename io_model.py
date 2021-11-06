from ocean_mask import levitus98
from memory import used_memory
import xarray as xr
import numpy as np
import xesmf as xe
import os
import warnings
import datetime
import dask

def era5_daily_io(lon_lim,lat_lim,year_lim):
    """
    The data io for calculating q2m, del_q, qsurf, and wind speed. 
    If there are more needed model variables, a manual 
    added variable in the setting section is available.
    
    The ERA5 daily output range from 1979 to 2020. The year limit 
    argument is to let the user to set their time period based on 
    different analysis period.
    
    The io function also calculate the 
        1. surface saturated specific humidity (qsurf)
        2. specific humidity difference between qsurf and huss (del_q)
        3. wind speed (sfcWind)
    
    To acomplish the calculation of qsurf
        - sst (surface temperature)
        - msl (sea level pressure)
    are nesessary variables in this model io. 
    
    To acomplish the calculation of q2m
        - d2m (dew point temperature)
        - msl (sea level pressure)
    are nesessary variables in this model io. 
    
    To acomplish the calculation of sfcWind
        - u10 (surface wind in x direction)
        - v10 (surface wind in y direction)
    are nesessary variables in this model io. 
    
    Inputs:
    =================
    lon_lim (list) : a list object containing the min and max value of 
                     longitude of the desired region.
                     
    lat_lim (list) : a list object containing the min and max value of 
                     latitude of the desired region.
                     
    year_lim (list) : a list object containing the min and max value of 
                     year of the desired period.      
                     
                     
    Outputs:
    =================
    ds_all_ts (xr.Dataset) : a xarray dataset in the form of dask array 
                     
    
                     
    """
    warnings.simplefilter("ignore")

    dirpath = '../data/ERA5/'
    files = '*_daily.nc'
    
    varlist = ['msl','d2m','u10','v10','sst','slhf','tp']
    
    ds_atm = xr.open_mfdataset(os.path.join(dirpath,files),chunks={'time':100,'latitude':100,'longitude':100})
    
    lon = ds_atm.longitude.values
    lon[np.where(lon<0)] += 360
    ds_atm['longitude'] = lon 
    
    # change varname to be consistent with model 
    ds_atm = ds_atm.rename({'slhf':'hfls','longitude':'lon','latitude':'lat'})
    
    # make lon to increase monotonicly (0-360)
    #   this is affected by changing -180-180 to 0-360 
    #   in io function
    ind = ds_atm.lon.diff(dim='lon').argmin().values
    ds_atm = ds_atm.roll(lon=ind)
    ################################################################################
    # cropping dataset
    
    # data longitude is -180 - 180
    
    
#     ds_atm_ts = xr.Dataset()
#     for var in varlist:
#         print(var)
    ds_atm = ds_atm.where((ds_atm.lon>=np.array(lon_lim).min())&
                             (ds_atm.lon<=np.array(lon_lim).max())&
                             (ds_atm.lat>=np.array(lat_lim).min())&
                             (ds_atm.lat<=np.array(lat_lim).max())&
                             (ds_atm['time.year']>=np.array(year_lim).min())&
                             (ds_atm['time.year']<=np.array(year_lim).max()),
                             drop=True)
    
    used_memory()
    ds_atm = ds_atm.persist()
    used_memory()


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
    da_q_surf = ds_atm['msl'].copy()*np.nan
    da_q2m = ds_atm['msl'].copy()*np.nan

    q2m = metpy.calc.specific_humidity_from_dewpoint(ds_atm['msl'].values*units.Pa,
                                                     ds_atm['d2m'].values*units.K
                                                     )

    mixing_ratio_surf = metpy.calc.saturation_mixing_ratio(ds_atm['msl'].values*units.Pa,
                                                           ds_atm['sst'].values*units.K)

    q_surf = metpy.calc.specific_humidity_from_mixing_ratio(mixing_ratio_surf)

    # unit for mixing ratio and specific humidity is kg/kg
    da_q_surf.values = q_surf.magnitude
    da_q2m.values = q2m.magnitude
    ds_atm['qsurf'] = da_q_surf
    ds_atm['huss'] = da_q2m
    ds_atm['del_q'] = ds_atm['qsurf']-ds_atm['huss']
    ds_atm['sfcWind'] = np.sqrt(ds_atm['u10']**2+ds_atm['v10']**2).compute()
    ds_atm['hfls'] = (-ds_atm['hfls']/60./60.).compute()
    used_memory()
    
    return ds_atm


def cmip6_cesm2_daily_io(lon_lim,lat_lim,year_lim):
    """
    The model io for latent heat flux corrections. 
    If there are more needed model variables, a manual 
    added variable in the setting section is available.
    
    The CESM2 daily output from CMIP6 archive in the historical run
    range from 1850 to 2014. The year limit argument is to let the user 
    to set their time period based on different analysis period.
    
    The io function also calculate the 
        1. surface saturated specific humidity (qsurf)
        2. specific humidity difference between qsurf and huss (del_q)
    
    To acomplish the calculation of qsurf
        - ts (surface temperature)
        - psl (sea level pressure)
    are nesessary variables in this model io. 
    
    Inputs:
    =================
    lon_lim (list) : a list object containing the min and max value of 
                     longitude of the desired region.
                     
    lat_lim (list) : a list object containing the min and max value of 
                     latitude of the desired region.
                     
    year_lim (list) : a list object containing the min and max value of 
                     year of the desired period.      
                     
                     
    Outputs:
    =================
    ds_all_ts (xr.Dataset) : a xarray dataset in the form of dask array 
                     
    
                     
    
    
    """

    warnings.simplefilter("ignore")

    # file name setting
    mip_era = 'CMIP6'
    activity_drs = 'CMIP'
    institution_id = 'NCAR'
    source_id = 'CESM2'
    experiment_id = 'historical'
    member_id = 'r1i1p1f1'
    grid_label = 'gn'
    version = '1'


    data_paths = {}
    data_files = {}
    data_vars = ['huss',
                 'ts',
                 'psl',
                 'sfcWind',
                 'hfls',
                 'pr']

    data_tables = ['day',
                   'Eday',
                   'day',
                   'day',
                   'day',
                   'day']

    for i in range(len(data_vars)):
        data_paths[data_vars[i]] = os.path.join('../data/CMIP6',mip_era,activity_drs,institution_id,
                                                     source_id,experiment_id,member_id,
                                                     data_tables[i],data_vars[i],grid_label,version)
        data_files[data_vars[i]] = '%s_%s_%s_%s_%s_%s_*.nc'%(data_vars[i],
                                                                         data_tables[i],
                                                                         source_id,
                                                                         experiment_id,
                                                                         member_id,
                                                                         grid_label)

    for nvar,var in enumerate(data_vars):
        ds_temp = xr.open_mfdataset(os.path.join(data_paths[var],data_files[var]),
                                         chunks={'time':100,'lat':100,'lon':100})
        if nvar == 0:
            ds_atm = ds_temp.copy()
        else:
            ds_atm = xr.merge([ds_atm,ds_temp],compat='override')


#     # import Pacific Basin
#     da_pacific = levitus98(ds_atm,
#                            basin=['pac'],
#                            reuse_weights=False, 
#                            newvar=True, 
#                            lon_name='lon',
#                            lat_name='lat', 
#                            new_regridder_name='')

    ################################################################################
    # cropping dataset

    ds_atm_ts=xr.Dataset()
    for nvar,var in enumerate(data_vars):
        ds_atm_ts[var] = (ds_atm[var])\
                                    .where((ds_atm[var].lon>=np.array(lon_lim).min())&
                                           (ds_atm[var].lon<=np.array(lon_lim).max())&
                                           (ds_atm[var].lat>=np.array(lat_lim).min())&
                                           (ds_atm[var].lat<=np.array(lat_lim).max())&
                                           (ds_atm[var]['time.year']>=np.array(year_lim).min())&
                                           (ds_atm[var]['time.year']<=np.array(year_lim).max()),
                                           drop=True)

    ds_all_ts = ds_atm_ts


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
    da_q_surf = ds_all_ts['huss'].copy()*np.nan

    mixing_ratio_surf = metpy.calc.saturation_mixing_ratio(ds_all_ts['psl'].values*units.Pa,
                                                           ds_all_ts['ts'].values*units.K)
    q_surf = metpy.calc.specific_humidity_from_mixing_ratio(mixing_ratio_surf)

    # unit for mixing ratio and specific humidity is kg/kg
    da_q_surf.values = q_surf.magnitude
    ds_all_ts['qsurf'] = da_q_surf
    ds_all_ts['del_q'] = ds_all_ts['qsurf']-ds_all_ts['huss']

    return ds_all_ts


def e3sm_most_daily_io(lon_lim,lat_lim,year_lim):
    """
    The model io for latent heat flux corrections. 
    If there are more needed model variables, a manual 
    added variable in the setting section is available.
    
    This is E3SM daily output from CMIP6 archive in the historical run
    range from 1995 to 2014 provided by the UW group. The year limit 
    argument is to let the user to set their time period based on 
    different analysis period.
    
    The MOST corrected surface variables is also needed for this io
        indir = '../data/processed/trop_vertical_prof/MOST_corr/'
        infile = 'E3SM_Historical_H1_global_????.nc'
    The mean bias needed for the MOST correction is also needed for this 
        indir = '../data/processed/trop_vertical_prof/MOST_corr/'
        infile = 'Meanfield_E3SM-1-0_historical_r1i1p1f1_gr_185001-201412.nc'
    
    The io function also calculate the 
        1. surface saturated specific humidity (qsurf)
        2. specific humidity difference between qsurf and huss (del_q)
    
    To acomplish the calculation of qsurf
        - ts (surface temperature)
        - psl (sea level pressure)
    are nesessary variables in this model io. 
    
    Inputs:
    =================
    lon_lim (list) : a list object containing the min and max value of 
                     longitude of the desired region.
                     
    lat_lim (list) : a list object containing the min and max value of 
                     latitude of the desired region.
                     
    year_lim (list) : a list object containing the min and max value of 
                     year of the desired period.      
                     
                     
    Outputs:
    =================
    ds_all_ts (xr.Dataset) : a xarray dataset in the form of dask array 
                     
    """
    
                     
    # file name setting
    data_paths = {}
    data_files = {}
    data_vars = ['PS',
                 'TS',
                 'LHFLX',
                 'PRECT']

    data_tables = ['day',
                   'day',
                   'day',
                   'day']

    for i in range(len(data_vars)):
        data_paths[data_vars[i]] = os.path.join('../data/','E3SM_historical_H1')
        data_files[data_vars[i]] = 'E3SM_Historical_H1_1995_2014_%s_%s.nc'%(data_tables[i],data_vars[i])

    for nvar,var in enumerate(data_vars):
        ds_temp = xr.open_dataset(os.path.join(data_paths[var],data_files[var]),
                                         chunks={'time':100,'lat':100,'lon':100})
        if nvar == 0:
            ds_atm = ds_temp.copy()
        else:
            ds_atm = xr.merge([ds_atm,ds_temp])


    timeax = []
    for nt,t in enumerate(ds_atm.time):
        year = int(t*1e-4)
        month = int((t-year*1e4)*1e-2)
        day = int((t-year*1e4-month*1e2))
        timeax.append(datetime.datetime(year,month,day))
    ds_atm['time']=timeax

#     # import Pacific Basin
#     da_pacific = levitus98(ds_atm,
#                            basin=['pac'],
#                            reuse_weights=False, 
#                            newvar=True, 
#                            lon_name='lon',
#                            lat_name='lat', 
#                            new_regridder_name='')

    ds_atm_trop=xr.Dataset()
    for nvar,var in enumerate(data_vars):
        ds_atm_trop[var] = (ds_atm[var])\
                                        .where((ds_atm[var].lon>=np.array(lon_lim).min())&
                                               (ds_atm[var].lon<=np.array(lon_lim).max())&
                                               (ds_atm[var].lat>=np.array(lat_lim).min())&
                                               (ds_atm[var].lat<=np.array(lat_lim).max())&
                                               (ds_atm[var]['time.year']>=np.array(year_lim).min())&
                                               (ds_atm[var]['time.year']<=np.array(year_lim).max()),
                                               drop=True)



    # read MOST corrected file from E3SM
    #  utilized the dask array 
    indir = '../data/processed/trop_vertical_prof/MOST_corr/'
    infile = 'E3SM_Historical_H1_global_????.nc'
    ds_temp2 = xr.open_mfdataset(indir+infile,
                                 combine='by_coords',
                                 concat_dim='time',
                                 chunks={'time':100,'lat':100,'lon':100})

    # meanfield (based on monthly) file from E3SM(CMIP6)
    indir = '../data/processed/trop_vertical_prof/MOST_corr/'
    infile = 'Meanfield_E3SM-1-0_historical_r1i1p1f1_gr_185001-201412.nc'
    ds_temp1 = xr.open_dataset(indir+infile)

    da_q2m_mean = ds_temp1['huss']
    da_u10m_mean = ds_temp1['sfcWind']

    # create regridder 
    regridder = xe.Regridder(ds_temp1,
                              ds_atm,
                              'bilinear',
                              filename='e3smCMIP62e3sm.nc',
                              periodic=True,
                              reuse_weights=False)

    # regrid correction file to E3SM grid
    da_q2m_mean = regridder(da_q2m_mean)
    da_u10m_mean = regridder(da_u10m_mean)

    da_q2m_bias = ds_temp2['q2m'].mean(dim='time').compute()-da_q2m_mean
    da_u10m_bias = ds_temp2['u10m'].mean(dim='time').compute()-da_u10m_mean

    ds_temp2['q2m'] = ds_temp2['q2m']-da_q2m_bias
    ds_temp2['u10m'] = ds_temp2['u10m']-da_u10m_bias

    ds_temp2['q2m'] = ds_temp2['q2m'].where(ds_temp2['q2m']>0)
    ds_temp2['u10m'] = ds_temp2['u10m'].where(ds_temp2['u10m']>0)

    ds_atm = xr.merge([ds_atm_trop,ds_temp2], join='inner')

    ds_atm = ds_atm.rename({'u10m':'sfcWind','q2m':'huss','LHFLX':'hfls','PRECT':'pr'})


    ds_all_ts = ds_atm


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

    mixing_ratio_surf = metpy.calc.saturation_mixing_ratio(ds_all_ts['PS'].values*units.Pa,
                                                           ds_all_ts['TS'].values*units.K)
    q_surf = metpy.calc.specific_humidity_from_mixing_ratio(mixing_ratio_surf)

    # unit for mixing ratio and specific humidity is kg/kg
    da_q_surf.values = q_surf.magnitude
    ds_all_ts['qsurf'] = da_q_surf
    ds_all_ts['del_q'] = ds_all_ts['qsurf']-ds_all_ts['huss']
    ds_all_ts['del_q'] = ds_all_ts['del_q'].where(ds_all_ts['del_q']>0.)    # not necessary true but based on observation


    return ds_all_ts


def UA_daily_io(lon_lim,lat_lim,case=None):

    """
    The model io for latent heat flux corrections. 
    If there are more needed model variables, a manual 
    added variable in the setting section is available.

    This is E3SM daily output from the UA group. The year limit 
    argument is to let the user to set their time period based on 
    different analysis period.

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

    if case in ['UA'] :
        casename = '20190521.v1_UA.FC5AV1C-04P2.ne30.cori-knl'
    elif case in ['CTL']:
        casename = '20190521.v1_ctl.FC5AV1C-04P2.ne30.cori-knl'
    elif case in ['COARE']:
        casename = '20190621.v1_COARE.FC5AV1C-04P2.ne30.cori-knl'
    else :
        print('Please enter casename')
    #     return

    # file name setting
    data_h1_vars = ['PS']
    data_h2_vars = ['TS',
                    'LHFLX',
                    'PRECC',
                    'PRECL',
                    'QBOT',
                    'U10']

    data_path = os.path.join('/maloney-scratch/joedhsu/proj1/data/jeyre/www/',
                                                casename,
                                                'global/cscratch1/sd/jeyre/acme_scratch/cori-knl/archive/',
                                                casename,
                                                'atm/hist/regrid/')
    data_h1_files = '%s.cam.h1.000?-??-??-00000.nc'%(casename)
    data_h2_files = '%s.cam.h2.000?-??-??-00000.nc'%(casename)

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
        
    
    if realm is 'atm':
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
        
    elif realm is 'ocn':
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

