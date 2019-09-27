import os
from glob import glob
import geopandas as gpd
from whitebox import WhiteboxTools

DEM_TILES = '/mnt/nrcs/isgs/output/dems/DEM_tindex.gpkg'
LAS_TILES = 'mnt/nrcs/isgs/lidar/LAS_tindex.gpkg'

def merge_tiles(tile, tindex):
    
    

    #wbt = WhiteboxTools()
    #os.chdir(las_files)
    #tifs = glob(os.path.join(las_files, '*.tif'))
    #tif_str = ';'.join(tifs)
    #wbt.mosaic(inputs=tif_str, output='/app/mosaic.tif', method='nn')
    



def main():
    
    dems = gpd.read_file(DEM_TILES)
    for row in dems.itertuples


if __name__ == '__main__':
    main()
