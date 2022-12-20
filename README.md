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
Therefore the data needs to be downloaded by running the `download_grid.sh` file.