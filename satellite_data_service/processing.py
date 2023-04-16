import logging
import os
import pathlib
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
import rasterio
import rasterio.mask
import rasterio.warp
import yaml
from bs4 import BeautifulSoup
from shapely import Point, Polygon, box


@dataclass
class SentinelData:
    image: rasterio.DatasetReader
    spatial_resolution: int

class SentinelImageProcessor:
    image_drivers = {
        'jp2': 'JP2OpenJPEG'
    }

    def __init__(self) -> None:
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        config = yaml.safe_load(open('config.yml'))['processing']
        self.data_dir: str = config['data-dir']
    
    def remove(self, id:str):
        filepath = os.path.join(self.data_dir, f'{id}.zip')
        if os.path.exists(filepath):
            self.logger.debug(f'Removing zip for id {id}..')
            os.remove(filepath)

    def process(self, input_dir:str, id:str, bands:List[str], locations:gpd.GeoDataFrame, radius:float) -> str:
        if not os.path.exists(input_dir):
            raise ValueError(f'No such directory: {input_dir}')
        if not bands:
            raise ValueError('Bands may not be empty')
        if len(locations) == 0:
            raise ValueError('Locations may not be empty')
        self.logger.debug(f'Column names: {locations.columns}')
        if 'name' not in locations.columns:
            raise ValueError('Locations must have a "name" column that will be used for the filenames')
        if radius < 0.0:
            raise ValueError('Radius may not be negative')
        # Prepare path hierarchy
        metadata_filepath = os.path.join(input_dir, next(filter(lambda x: x.startswith('MTD'), os.listdir(input_dir))))
        self.logger.debug(f'Metadata path: {metadata_filepath}')
        images_dir = os.path.join(input_dir, 'GRANULE')
        images_dir = os.path.join(images_dir, os.listdir(images_dir)[0], 'IMG_DATA')
        self.logger.debug(f'Image path: {images_dir}')
        # Get spatial resolutions per band - 10 m, 20 m, 60 m
        spatial_resolutions = self.get_spatial_resolutions(metadata_filepath)
        self.logger.debug(f'Bands: {bands}')
        bands_meta_style: List[str] = [self.get_band_name_for_meta(band) for band in bands]
        # Ensure all band names are valid
        malformed_bands = list(map(lambda tuple: bands[tuple[0]],
            filter(lambda tuple: tuple[1] not in spatial_resolutions, enumerate(bands_meta_style))))
        if any(malformed_bands):
            raise ValueError(f'Band names {malformed_bands} do not exist')
        # Load data for all bands
        data = {band: self.load_band_data(images_dir, spatial_resolutions, self.get_band_name_for_files(band))
            for band in bands_meta_style}
        all_crs = [band.image.crs for band in data.values()]
        if len(set(all_crs)) > 1:
            raise RuntimeError(f'Expected a single CRS, but got multiple: {all_crs}')
        used_crs = all_crs[0]
        if used_crs is None:
            raise ValueError('The sentinel data does not have a CRS set! Should never happen!')
        self.logger.debug(f'Locations CRS: {locations.crs}, Sentinel CRS: {used_crs}')
        # Transform locations to CRS of sentinel data
        if locations.crs != used_crs:
            locations['geometry'] = locations['geometry'].apply(
                lambda position: self.reproject_point(position, locations.crs, used_crs))
        # Build areas of obeservation around locations
        locations['bbox'] = locations['geometry'].apply(
            lambda position: self.get_area_of_observation(position, radius))
        # Remove bounding boxes that are not completely contained in the data bounds
        data_bounds = list(data.values())[0].image.bounds
        data_bounds: Polygon = box(data_bounds.left, data_bounds.bottom, data_bounds.right, data_bounds.top)
        self.logger.debug(f'Data bounds: {data_bounds}')
        locations['contained'] = locations['bbox'].apply(lambda bbox: data_bounds.contains(bbox))
        outside_locations = locations[~locations['contained']]
        self.logger.debug(f'The bounding boxes of the following locations are outside the data bounds: {outside_locations.name}')
        locations = locations[locations['contained']]
        # Extract data in bounding boxes from whole data and write to files
        out_dir = os.path.join(self.data_dir, id)
        os.makedirs(out_dir, exist_ok=True)
        for band_name, band in data.items():
            out_band_dir = os.path.join(out_dir, band_name)
            os.makedirs(out_band_dir, exist_ok=True)
            out_data: pd.Series = locations['bbox'].apply(lambda bbox: self.transform_image(band.image, bbox))
            named_out_data = pd.DataFrame({'name': locations['name'], 'out': out_data})
            named_out_data.apply(lambda row: self.image_to_file(
                    row['out'][0],
                    row['out'][1],
                    row['out'][2],
                    os.path.join(out_band_dir, row['name'] + '.jp2'),
                    SentinelImageProcessor.image_drivers['jp2']
                ), axis=1)
        # Pack all data in a zip file
        archive = shutil.make_archive(out_dir, 'zip', root_dir=self.data_dir, base_dir=id)
        # Remove source folder
        shutil.rmtree(out_dir)
        return archive
    
    def get_band_name_for_files(self, name:str) -> str:
        if len(name) < 3:
            # B7 to B07
            return name[0] + '0' + name[1:]
        return name

    def get_band_name_for_meta(self, name:str) -> str:
        if name[1] == '0':
            # B07 to B7
            return name[0] + name[2:]
        return name
    
    def get_spatial_resolutions(self, metadata_filepath:str) -> Dict[str, int]:
        with open(metadata_filepath, 'r') as file:
            metadata = file.read()
        metadata_soup = BeautifulSoup(metadata, 'xml')
        spectral_infos = metadata_soup.find_all('Spectral_Information')
        spatial_resolutions = {}
        for info in spectral_infos:
            band_name = info.get('physicalBand')
            spatial_resolutions[band_name] = int(info.RESOLUTION.string)
        self.logger.debug(f'Spatial resolutions: {spatial_resolutions}')
        return spatial_resolutions
    
    def get_image_path(self, images_dir:str, name:str) -> str:
        filename = next(filter(lambda img: img.find(name) >= 0, os.listdir(images_dir)))
        return os.path.join(images_dir, filename)

    def open_image(self, images_dir:str, name:str) -> rasterio.DatasetReader:
        image_path = self.get_image_path(images_dir, name)
        return rasterio.open(image_path,
            driver=SentinelImageProcessor.image_drivers[pathlib.Path(image_path).suffix[1:]])

    def load_band_data(self, images_dir:str, spatial_resolutions:Dict[str, int], band_name:str) -> SentinelData:
        image = self.open_image(images_dir, self.get_band_name_for_files(band_name))
        spatial_resolution = spatial_resolutions[self.get_band_name_for_meta(band_name)]
        return SentinelData(image, spatial_resolution)
    
    def reproject_point(self, point:Point, crs_from, crs_to) -> Point:
        return Point(rasterio.warp.transform_geom(crs_from, crs_to, point)['coordinates'])
    
    def get_area_of_observation(self, point:Point, radius:float) -> Polygon:
        return box(
            point.x - radius,
            point.y - radius,
            point.x + radius,
            point.y + radius
        )
    
    def transform_image(self, band: rasterio.DatasetReader, bounding_box:Polygon):
        out_image, out_transform = rasterio.mask.mask(band, [bounding_box], crop=True)
        out_meta = band.meta
        return out_image, out_transform, out_meta
    
    def image_to_file(self, out_image, out_transform, out_meta, filepath:str, driver:str):
        out_meta.update({
            "driver": driver,
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })
        self.logger.debug(f'Writing to file {filepath}')
        with rasterio.open(filepath, "w", **out_meta) as dest:
            dest.write(out_image)