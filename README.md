# Satellite Data Service
This service provides access to measurement-data of the [Sentinel-1](https://sentinel.esa.int/web/sentinel/missions/sentinel-1) and [Sentinel-2](https://sentinel.esa.int/web/sentinel/missions/sentinel-2) satellites.
Additionally, operations on the surface-subdividing geometry "grid" are provided.

## How to run
For now, this remains a manual task.
To start the service navigate into the root directory of this repository and enter the following command into a terminal
```
uvicorn satellite_data_service.main:app --port 8001 --reload
```

## Notes
Currently, the grid is not automatically downloaded, if missing.
Therefore the data needs to be manually downloaded from the official [ESA website](https://sentinel.esa.int/documents/247904/1955685/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml/ec05e22c-a2bc-4a13-9e84-02d5257b09a8) once and then placed into a `data/` folder under this modules' root directory.