#!/bin/bash

dataDir="data"
fileName="sentinel_2_level_1c_tiling_grid.kml"
sentinelGridLink="https://sentinel.esa.int/documents/247904/1955685/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml/ec05e22c-a2bc-4a13-9e84-02d5257b09a8"

filePath="$dataDir/$fileName"

if  [ -f "$filePath" ]; then
    echo "The Sentinel grid is already available. Aborting.."
else
    if [ -d "$dataDir" ]; then
        echo "Directory $dataDir already exists"
    else
        echo "Creating directory $dataDir"
        mkdir $dataDir
    fi
    echo "Downloading the Sentinel grid.."
    wget -O $filePath $sentinelGridLink
    echo "Complete!"
fi