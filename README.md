# Satellite Data Service
This service provides access to measurement-data of the [Sentinel-1](https://sentinel.esa.int/web/sentinel/missions/sentinel-1) and [Sentinel-2](https://sentinel.esa.int/web/sentinel/missions/sentinel-2) satellites.
Additionally, operations on the surface-subdividing geometry "grid" are provided.

## Setup
When not running this service via docker compose like in [AIMLESS](https://git.scc.kit.edu/master-thesis-ai-ml-based-support-for-satellite-exploration/aimlsse), first create a volume:
```
docker volume create --name satellite-data-storage
```
Then run the container and use the volume:
```
docker run -d -p 8000:8000 -v satellite-data-storage:/aimlsse/app/grid satellite-data-service
```
Here `/aimlsse/app/` is the working directory of the service, while `grid` is the subdirectory for the data that is specified in the `config.yml` file.
If the path in the config is changed, update the container-side binding of the volume in the command above as well.