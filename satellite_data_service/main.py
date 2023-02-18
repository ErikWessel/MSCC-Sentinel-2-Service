import json
import logging
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from typing import List, Union

import geopandas as gpd
import shapely
from aimlsse_api.interface import SatelliteDataAccess
from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from shapely import Point, Polygon

from . import CopernicusAccess, LocationToGridCellsMapper, RequestScheduler

logging.basicConfig(level=logging.DEBUG)
app = FastAPI()
security = HTTPBasic()

class SatelliteDataService(SatelliteDataAccess):
    def __init__(self) -> None:
        super().__init__()
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        # Setup a router for FastAPI
        self.router = APIRouter()
        self.router.add_api_route('/queryContainingGeometry', self.queryContainingGeometry, methods=['POST'])
        self.router.add_api_route('/queryProductsMetadata', self.queryProductsMetadata, methods=['POST'])
        self.router.add_api_route('/requestProduct', self.requestProduct, methods=['GET'])
        self.router.add_api_route('/extractFeatures', self.extractFeatures, methods=['POST'])
        self.router.add_api_route('/getProduct', self.getProduct, methods=['GET'])
        
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

    async def queryProductsMetadata(self, footprint:str, datetime_from:datetime, datetime_to:datetime,
        credentials:HTTPBasicCredentials = Depends(security)):
        # Convert footprint to shape and assert type
        footprint_geometry = shapely.from_wkt(footprint)
        assert isinstance(footprint_geometry, (Point, Polygon))
        # Query products
        ca = CopernicusAccess(credentials.username, credentials.password)
        logging.info(f'Querying products from {datetime_from} to {datetime_to} in footprint {footprint_geometry}..')
        data = ca.search(footprint_geometry, datetime_from, datetime_to)
        logging.info('Query for products complete!')
        return JSONResponse(json.loads(data.to_json()))
    
    async def requestProduct(self, id:str, credentials:HTTPBasicCredentials = Depends(security)):
        scheduler = RequestScheduler()
        state = scheduler.request(id, credentials.username, credentials.password)
        scheduler.store_schedule()
        return JSONResponse({
            'id': id,
            'state': state.value
        })
    
    async def extractFeatures(self, id:str, radius:float, data:dict):
        self.validate_json_parameters(data, [['bands'], ['locations'], ['crs']])
        self.logger.debug(f'Starting feature-extraction for id {id} with radius {radius} m and data:\n{data}')
        bands: List[str] = data['bands']
        locations: gpd.GeoDataFrame = gpd.GeoDataFrame.from_features(data['locations'], crs=data['crs'])
        try:
            zip_filepath = RequestScheduler().process_data_for_request(id, bands, locations, radius)
        except ValueError as error:
            self.logger.debug(error)
            return PlainTextResponse(error, status_code=HTTPStatus.BAD_REQUEST)
        self.logger.debug(f'Path of zip-file: {zip_filepath}')
        return FileResponse(zip_filepath, filename=f'{id}.zip')

    async def getProduct(self, id:str):
        try:
            zip_filepath = RequestScheduler().get_raw_product(id)
        except ValueError as error:
            self.logger.debug(error)
            return PlainTextResponse(error, status_code=HTTPStatus.BAD_REQUEST)
        self.logger.debug(f'Path of zip-file: {zip_filepath}')
        return FileResponse(zip_filepath, filename=Path(zip_filepath).name)

    def validate_json_parameters(self, data:dict, parameters:List[List[str]]) -> List[List[str]]:
        '''
        Ensures that the given JSON dict contains the specified parameters.
        Each entry in the parameters is a list of strings, where at least one must be present.

        Parameters
        ----------
        data: `JSON / dict`
            The JSON inside which the parameters are searched for
        parameters: `List[List[str]]`
            The parameters to search for inside the JSON

        Raises
        ------
        `ValueError`
            If a parameter is not contained in the data
        
        Returns
        -------
        `List[List[str]]`
            All parameters that are present in the data
        '''
        parameters_present = []
        for attributes in parameters:
            attributes_present = list(filter(lambda x: x in data, attributes))
            if len(attributes_present) == 0:
                raise ValueError(f'Missing information about "{attributes}" from received JSON data.')
            else:
                parameters_present += [attributes_present]
        return parameters_present

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('fiona').setLevel(logging.INFO)
logging.getLogger('rasterio').setLevel(logging.INFO)
satelliteDataService = SatelliteDataService()
app.include_router(satelliteDataService.router)