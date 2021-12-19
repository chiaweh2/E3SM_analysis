#!/bin/bash
mapfile=../../../E3SM_regrid/mapfiles/map_ne30np4_to_cmip6_180x360_aave.20181001.nc                    # path to appropriate mapfile
input_dir=../run/*.A_WCYCL20TRS_CMIP6.ne30_oECv3_ICG.cori-knl.cam.h*.nc                  # input directory path
output_dir=./                                                        # output directory path

ncremap -m ${mapfile} -i ${input_dir} -O ${output_dir}
