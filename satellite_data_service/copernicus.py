import logging
import os
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Dict, List, Optional, Union
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
import pandas as pd
import schedule
import yaml
from sentinelsat import (InvalidKeyError, LTAError, LTATriggered, SentinelAPI, ServerError, InvalidChecksumError)
from shapely import Point, Polygon

from . import SentinelImageProcessor, LocationToGridCellsMapper

from aimlsse_api.data import QueryStates

class RequestScheduler(object):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(RequestScheduler, cls).__new__(cls)
            cls.initialized = False
        return cls.instance
    
    def __init__(self) -> None:
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        if RequestScheduler.initialized:
            self.logger.debug('Already initialized - skipping')
        else:
            self.logger.debug('Initializing..')
            config = yaml.safe_load(open('config.yml'))['copernicus']
            self.data_dir: str = config['data-dir']
            self.schedule_filepath: str = config['schedule-filepath']
            os.makedirs(self.data_dir, exist_ok=True)
            os.makedirs(os.path.dirname(self.schedule_filepath), exist_ok=True)
            self.schedule: pd.DataFrame = None
            if os.path.exists(self.schedule_filepath):
                self.schedule = pd.read_csv(self.schedule_filepath, index_col='id')
            else:
                self.schedule = pd.DataFrame(columns=['state', 'last_query', 'title'])
                self.schedule.index.name = 'id'
            self.logger.debug(self.schedule)
            self.active_requests: Dict[str, schedule.Job] = {}
            self.run_as_thread(self.run_scheduler)
            RequestScheduler.initialized = True

    def run_as_thread(self, function:Callable):
        thread = threading.Thread(target=function)
        thread.start()

    def run_scheduler(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

    def store_schedule(self):
        self.schedule.to_csv(self.schedule_filepath, index_label='id')

    def __get_request(self, id:str) -> Optional[pd.DataFrame]:
        try:
            return self.schedule.loc[id]
        except KeyError:
            return None
    
    def __get_requests_by_state(self, state:QueryStates) -> pd.DataFrame:
        return self.schedule[self.schedule['state'] == state]

    def request(self, id:str, username:str, password:str) -> QueryStates:
        api = SentinelAPI(username, password)
        request = self.__get_request(id)
        self.logger.debug(f'Request for id {id} was {request}')
        if request is None:
            try:
                metadata = api.get_product_odata(id)
            except InvalidKeyError:
                self.logger.warning(f'Product with id {id} does not exist online - request will not be made')
                return QueryStates.INVALID
            new_request = {
                'state': QueryStates.NEW.value,
                'last_query': None,
                'title': metadata['title']
            }
            self.schedule = pd.concat([self.schedule, pd.DataFrame(new_request, index=[id])])
            self.logger.info(f'Added new request: {new_request}')
        else:
            state = request['state']
            self.logger.info(f'Request already made - state is: {state}')
        request = self.__get_request(id)
        state = request['state']
        should_download = id not in self.active_requests and self.__should_download(state)
        self.logger.debug(f'State is {state} - should download? {should_download}')
        if should_download:
            self.logger.info(f'Try to download directly..')
            if not self.__try_download_with_local_checks(id, username, password):
                self.logger.info(f'Data unavailable - starting download schedule..')
                job = schedule.every(30).minutes.do(self.__try_download_with_local_checks, id, username, password)
                self.active_requests[id] = job
            request = self.__get_request(id)
            state = request['state']
        return QueryStates(state)

    def __assert_request_available(self, id:str):
        request = self.__get_request(id)
        if request is None:
            raise ValueError(f'There is no request with id {id}')
        state = QueryStates(request['state'])
        if state != QueryStates.AVAILABLE:
            raise ValueError(f'Only requests of state {QueryStates.AVAILABLE.name} may be processed - actual state was {state}')
    
    def __unzip_product(self, id:str) -> str:
        request = self.__get_request(id)
        path_no_ext = os.path.join(self.data_dir, request['title'])
        filepath_zip = path_no_ext + '.zip'
        dirpath_safe = path_no_ext + '.SAFE'
        if not os.path.isdir(dirpath_safe):
            # Extract zip file like S2A_MSIL1C_20220104T103431_N0301_R108_T32UMA_20220104T123507.zip
            # to S2A_MSIL1C_20220104T103431_N0301_R108_T32UMA_20220104T123507.SAFE/
            with ZipFile(filepath_zip) as zip_file:
                zip_file.extractall(self.data_dir)
            # Remove the original zip file so we do not have duplicates
            os.remove(filepath_zip)
        return dirpath_safe

    def process_data_for_request(self, id:str, bands:List[str], locations:gpd.GeoDataFrame, radius:float) -> str:
        self.__assert_request_available(id)
        product_dir = self.__unzip_product(id)
        # Extract features from the sentinel data
        return SentinelImageProcessor().process(product_dir, id, bands, locations, radius)
    
    def get_raw_product(self, id:str) -> str:
        self.__assert_request_available(id)
        request = self.__get_request(id)
        return os.path.join(self.data_dir, request['title'] + '.zip')

    def __check_available(self, id:str) -> QueryStates:
        request = self.__get_request(id)
        if request is None:
            raise ValueError(f'There is no request for {id} - create one first')
        self.logger.debug(f'Checking availability for request {request}')
        files = list(filter(lambda x: x.startswith(request['title']), os.listdir(self.data_dir)))
        self.logger.debug(f'Found {len(files)} files for id {id}:\n{files}')
        if any(filter(lambda x: x.endswith('.zip') or x.endswith('.SAFE'), files)):
            return QueryStates.AVAILABLE
        if any(filter(lambda x: x.endswith('.incomplete'), files)):
            return QueryStates.INCOMPLETE
        return QueryStates(request['state'])

    def __should_download(self, state:QueryStates) -> bool:
        return state in [
            QueryStates.NEW,
            QueryStates.PENDING,
            QueryStates.INCOMPLETE
        ]

    def __try_download_sentinel_data(self, id:str, api:SentinelAPI):
        request = self.__get_request(id)
        self.logger.debug(f'Trying to download request\n{request} at index {request.index}')
        if request is None:
            raise ValueError(f'Unable to prepare download for id {id} - the request does not exist')
        old_state = request['state']
        try:
            self.schedule.loc[id]['state'] = QueryStates.INCOMPLETE.value
            self.logger.debug(f'Initiating download for id {id}')
            api.download(id, directory_path=self.data_dir)
            self.schedule.loc[id]['state'] = QueryStates.AVAILABLE.value
            if id in self.active_requests:
                job = self.active_requests.pop(id)
                schedule.cancel_job(job)
        except LTATriggered:
            self.schedule.loc[id]['state'] = QueryStates.PENDING.value
            self.logger.info(f'Data for id {id} is not available - a request to retrieve it from the LTA has been initiated')
        except LTAError:
            self.schedule.loc[id]['state'] = QueryStates.UNAVAILABLE.value
            self.logger.info(f'Data for id {id} is not available - no request could be initiated')
        except ServerError as error:
            self.logger.error(f'Copernicus server error: {error.msg}')
            if 'NullPointerException' in error.msg:
                self.schedule.loc[id]['state'] = QueryStates.UNAVAILABLE
            else:
                # The server is probably down for maintainance
                self.schedule.loc[id]['state'] = old_state
        except InvalidChecksumError:
            self.logger.error(f'Invalid checksum of download')
            self.schedule.loc[id]['state'] = old_state
            request = self.__get_request(id)
            filepath_zip = os.path.join(self.data_dir, request['title'] + '.zip')
            if os.path.exists(filepath_zip):
                os.remove(filepath_zip)
            else:
                self.schedule.loc[id]['state'] = QueryStates.UNAVAILABLE

        self.schedule.loc[id]['last_query'] = np.datetime64('now')

    def __try_download_with_local_checks(self, id:str, username:str, password:str) -> bool:
        api = SentinelAPI(username, password)
        request = self.__get_request(id)
        if request is None:
            raise ValueError(f'There is no request for {id} - create one first')
        # Check if data is locally available
        checked_state = self.__check_available(id)
        self.schedule.loc[id]['state'] = checked_state
        if not self.__should_download(checked_state):
            self.logger.debug('Data is available in storage')
            if id in self.active_requests:
                self.active_requests.remove(id)
            return
        self.logger.debug('Data unavailable - trying to download')
        self.logger.debug(f'Id: {id}, api: {api}')
        self.__try_download_sentinel_data(id, api)
        checked_state = self.__check_available(id)
        self.logger.debug(f'Current state is {checked_state} for id {id}')
        return checked_state == QueryStates.AVAILABLE

class CopernicusAccess():
    def __init__(self, username:str, password:str) -> None:
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        config = yaml.safe_load(open('config.yml'))['copernicus']
        self.search_url: str = config['search-url']
        self.username = username
        self.password = password
        self.set_api(SentinelAPI(username, password))
    
    def set_api(self, api:SentinelAPI) -> None:
        self.api = api

    def get_api(self) -> SentinelAPI:
        return self.api

    def is_api_set(self) -> bool:
        return self.api is not None

    def get_username(self) -> str:
        return self.username
    
    def get_password(self) -> str:
        return self.password

    def __asZulu(self, dt:datetime):
        return dt.astimezone(timezone.utc)

    def searchFootprint(self, footprint:Union[Point, Polygon], datetime_from:datetime, datetime_to:datetime) -> pd.DataFrame:
        api = self.get_api()
        products = api.query(
            footprint,
            date=(self.__asZulu(datetime_from), self.__asZulu(datetime_to)),
            platformname='Sentinel-2',
            producttype='S2MSI1C'
        )
        return api.to_dataframe(products)
    
    def searchCell(self, cell_name:str, datetime_from:datetime, datetime_to:datetime) -> pd.DataFrame:
        api = self.get_api()
        cell = LocationToGridCellsMapper().get_cell(cell_name)
        products = api.query(
            cell.centroid,
            date=(self.__asZulu(datetime_from), self.__asZulu(datetime_to)),
            platformname='Sentinel-2',
            producttype='S2MSI1C'
        )
        return api.to_dataframe(products)

    def query_image(self, id:str, band:str):
        pass