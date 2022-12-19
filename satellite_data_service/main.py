import geopandas as gpd
import logging
import datetime
import json
from fastapi import FastAPI, APIRouter, Request
from fastapi.responses import JSONResponse
from aimlsse_api.satellite_data_access import SatelliteDataAccess
from satellite_data_service.location_to_grid_cells_mapper import LocationToGridCellsMapper

class SatelliteDataAccessor(SatelliteDataAccess):
    def __init__(self) -> None:
        super().__init__()
        # Setup a router for FastAPI
        self.router = APIRouter()
        self.router.add_api_route('/queryContainingGeometry', self.queryContainingGeometry, methods=['POST'])
        self.router.add_api_route('/queryMeasurements', self.queryMeasurements, methods=['POST'])
        
        self.locationToGridCellsMapper = LocationToGridCellsMapper()

    async def queryContainingGeometry(self, locations:Request) -> JSONResponse:
        logging.info('Querying for geometry..')
        locations_json = await locations.json()
        logging.debug(f"location json contains: {str(locations_json)[:1000]}")
        locations_gdf = gpd.GeoDataFrame.from_features(locations_json['features'])
        # Fallback mechanism to reduce data to duplicate-free set of points. Sent data should already be free of duplicates.
        locations_gdf.drop_duplicates('geometry', inplace=True)
        # Map location points to grid cells
        grid_cells = self.locationToGridCellsMapper.selectLocationContainingGridCells(locations_gdf)
        logging.info('Query for geometry complete!')
        return JSONResponse(json.loads(grid_cells.to_json(drop_id=True)))

    async def queryMeasurements(self, datetime_from:datetime.datetime, datetime_to:datetime.datetime, locations:Request) -> JSONResponse:
        logging.info('Querying for measurements..')
        logging.info('Query for measurements complete!')
        # TODO - load data and return measurements
        raise NotImplementedError("Data access is not available yet!")

logging.basicConfig(level=logging.DEBUG)
app = FastAPI()
satelliteDataAccessor = SatelliteDataAccessor()
app.include_router(satelliteDataAccessor.router)