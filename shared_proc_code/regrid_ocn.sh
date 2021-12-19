#/bin/bash

startyear=2000
endyear=2000
#startmon=2
#endmon=7
mapfile=../../../E3SM_regrid/mapfiles/map_oEC60to30v3_to_cmip6_180x360_aave.20181001.nc         # path to appropriate mapfile

input_dir=../run/mpaso.hist.forcing*.nc
output_dir=./                                                                                   # output directory path
ncremap -m ${mapfile} -i ${input_dir} -O ${output_dir}

input_dir=../run/mpaso.hist.diag*.nc
ncremap -m ${mapfile} -i ${input_dir} -O ${output_dir}

i=$startyear
while [ $i -le $endyear ]
do 
    for j in {01..12}
    do
        mon=$j
        input_file=../run/mpaso.hist.3d_monthly.$i-$mon-01.nc
        tran_file=./trandir/mpaso.hist.3d_monthly.$i-$mon-01.nc
        output_file=./

        mkdir trandir
        ncpdq -a Time,nVertLevels,nCells ${input_file} ${tran_file}
        ncremap -m ${mapfile} -i ${tran_file} -O ${output_file}
        rm -rf ${tran_file}
    done
    let i=i+1
done
