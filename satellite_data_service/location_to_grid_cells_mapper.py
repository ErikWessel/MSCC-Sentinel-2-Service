import logging
from typing import Dict, List, Tuple
import geopandas as gpd

MappingType = Dict[Tuple[float, float], List[str]]
"""This type denotes a dict of (longitude, latitude) tuples for keys with a list of grid-cell-names"""

class LocationToGridCellsMapper ():
    """Provides mappings from location-points to the grid-cells in which they are located"""

    __grid: gpd.GeoDataFrame
    """
    The grid that is composed of polygons, which in turn cover a certain area of the planet.
    Instead of using this variable directly, use the get_grid() method to make sure it is properly initialized.
    """

    def __init__(self) -> None:
        self.__grid = None

    def load_grid(self):
        """Provides a way to initialize and reload the grid"""
        logging.info('Loading Sentinel-Grid..')
        gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw' # enable KML support
        self.__grid = gpd.read_file('data/sentinel_2_level_1c_tiling_grid.kml', driver='KML')
        logging.info('Sentinel-Grid loaded!')

    def get_grid(self) -> gpd.GeoDataFrame:
        """
        Provides access to the grid, while making sure that it is initialized

        Returns
        -------
        GeoDataFrame
            The grid that is composed of polygons, which in turn cover a certain area of the planet
        """
        if self.__grid is None:
            self.load_grid()
        return self.__grid

    def selectLocationContainingGridCells(self, locations:gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Maps location-points in the form of (longitude, latitude) to the grid cells in which they are located.
        The location-points were intentionally stored as a tuple of longitude and latitude values, since the type
        geopandas.geometry.Point is not hashable.

        The user is expected to have cleaned up the input data and to have removed any unwanted duplicates!
        This method maps every location-point to all grid-cells that it fits into, so be aware of the impact of
        large datasets.

        Parameters
        ----------
        locations: geopandas.GeoDataFrame
            The locations-points in the form of (longitude, latitude)
        
        Returns
        -------
        Dict[(float, float), List[str]]
            The resulting map from tuples of (longitude, latitude) to lists of grid-cell-names
        """
        grid = self.get_grid()
        logging.debug(grid)
        logging.info('Starting selection of grid-cells..')
        result = grid.loc[grid.geometry.apply(lambda tile: any(tile.contains(locations.geometry)))]
        logging.info('Selection of grid-cells complete!')
        logging.debug(f'Result of selection: {result}')
        return result

    def mapLocationsToContainingGridCellLabels(self, locations:gpd.GeoDataFrame) -> MappingType:
        """
        Maps location-points in the form of (longitude, latitude) to the grid cells in which they are located.
        The location-points were intentionally stored as a tuple of longitude and latitude values, since the type
        geopandas.geometry.Point is not hashable.

        The user is expected to have cleaned up the input data and to have removed any unwanted duplicates!
        This method maps every location-point to all grid-cells that it fits into, so be aware of the impact of
        large datasets.

        Parameters
        ----------
        locations: geopandas.GeoDataFrame
            The locations-points in the form of (longitude, latitude)
        
        Returns
        -------
        Dict[(float, float), List[str]]
            The resulting map from tuples of (longitude, latitude) to lists of grid-cell-names
        """
        grid = self.get_grid()
        logging.debug(grid)
        logging.info('Starting mapping of locations to grid-cells..')
        # TODO - Find more efficient way of mapping
        result = {}
        for point in locations.geometry:
            result[(point.x, point.y)] = [ tile['Name'] for index, tile in grid.iterrows() if tile.geometry.contains(point) ]
        logging.info('Mapping of locations to grid-cells complete!')
        logging.debug(f'Result of mapping: {result}')
        return result