import json
import logging
from datetime import datetime
from typing import Union

import geopandas as gpd
import shapely
from aimlsse_api.interface import SatelliteDataAccess
from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from shapely import Point, Polygon

from . import CopernicusAccess, LocationToGridCellsMapper, RequestScheduler

logging.basicConfig(level=logging.DEBUG)
app = FastAPI()
security = HTTPBasic()

class SatelliteDataService(SatelliteDataAccess):
    def __init__(self) -> None:
        super().__init__()
        # Setup a router for FastAPI
        self.router = APIRouter()
        self.router.add_api_route('/queryContainingGeometry', self.queryContainingGeometry, methods=['POST'])
        self.router.add_api_route('/queryMeasurements', self.queryMeasurements, methods=['POST'])
        self.router.add_api_route('/request', self.request, methods=['GET'])
        
        self.locationToGridCellsMapper = LocationToGridCellsMapper()

    async def queryContainingGeometry(self, locations:Request) -> JSONResponse:
        logging.info('Querying for geometry..')
        locations_json = await locations.json()
        logging.debug(f"location json contains: {str(locations_json)[:1000]}")
        locations_gdf = gpd.GeoDataFrame.from_features(locations_json['features'])
        # Fallback mechanism to reduce data to duplicate-free set of points. Sent data should already be free of duplicates.
        locations_gdf.drop_duplicates('geometry', inplace=True)
        # Map location points to grid cells
        grid_cells = self.locationToGridCellsMapper.mapLocationsToContainingGridCellLabels(locations_gdf)
        logging.info('Query for geometry complete!')
        return JSONResponse(json.loads(grid_cells.to_json(drop_id=True)))

    async def queryMeasurements(self, footprint:str, datetime_from:datetime, datetime_to:datetime,
    credentials: HTTPBasicCredentials = Depends(security)) -> JSONResponse:
        footprint_geometry = shapely.from_wkt(footprint)
        assert isinstance(footprint_geometry, (Point, Polygon))

        ca = CopernicusAccess(credentials.username, credentials.password)
        logging.info('Querying for measurements..')
        data = ca.search(footprint_geometry, datetime_from, datetime_to)
        logging.info('Query for measurements complete!')
        return JSONResponse(json.loads(data.to_json()))
    
    async def request(self, id:str, credentials: HTTPBasicCredentials = Depends(security)):
        ca = CopernicusAccess(credentials.username, credentials.password)
        scheduler = RequestScheduler()
        state = scheduler.request(id, credentials.username, credentials.password)
        scheduler.store_schedule()
        return PlainTextResponse(state.value)

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('fiona').setLevel(logging.INFO)
satelliteDataService = SatelliteDataService()
app.include_router(satelliteDataService.router)