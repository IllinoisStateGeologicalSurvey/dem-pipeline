import os
import sys
import time
from glob import glob
from dask.distributed import Client
import geopandas as gpd
import rasterio
import rasterio.fill 
import rasterio.merge
from raster.vrt import WarpedVRT

import pandas as pd
pd.options.mode.chained_assignment = None


DEM_TINDEX = '/mnt/nrcs/isgs/output/raw/DEM_tindex.gpkg'
LAS_TINDEX = '/mnt/nrcs/isgs/lidar/LAS_tindex.gpkg'


def merge_dummy(fname, bounds, frags):
    """dummy function for debugging purposes"""

    print('merge_dummy:', fname, bounds, type(frags), flush=True)
    print(frags.loc[1:3,'location'], flush=True)
    return True


def merge_simple(fname, bounds, frags):

    print('merge_simple:', fname, flush=True)
    try:
        sindex = frags.sindex
        overlap = frags.iloc[list(sindex.intersection(bounds))]
        files = ';'.join(overlap['location'])
    except Exception as e:
        files = str(e)
        
    f = open(fname, 'w')
    f.write(files+os.linesep)
    f.close()


def merge(df, crs):
    
    print(f'mosaic: merging {len(flist)} frags', flush=True)
    srcs = [rasterio.open(f) for f in df.location]
    vrts = [WarpedVRT(s, src_crs=c, crs=crs) for s,c in zip(srcs, df['srs']]
    nodata = srcs[0].nodata

    arr, transform = rasterio.merge.merge(vrts)
    arr = rasterio.fill.fillnodata(arr[0,:,:], np.where(arr==nodata, 0, 1))

    # TODO just del srcs for bettter cleanup?
    for s in srcs:
        s.close()

    return arr, transform


def merge_frags(fname, frags, bounds, crs)
    """Find the frags within the bounds and merge into numpy array

      Args:
        frags (GeoDataFrame): dataframe containing fragment metadata
        bounds (tuple(float*4)): limit frags to the bounds
        crs (): target CRS

      Returns:
        tuple(ndarray, ndarray): merged array and Affine transform
    """

    print(f'merge_frags:', flush=True)
    overlap = frags.iloc[list(frags.sindex.intersection(bounds))]
    try:
        arr, transform = merge(overlap, crs)
    except Exception as e:
        print(f'Error creating {fname}: missing frags', flush=True)

    with rasterio.open(fname, 'w') as f:
        f.crs = crs
        f.transform = transform
        f.nodata = -32768.0
        f.write(arr)


def get_target_tiles(fname, path, ext='.tif', exists=False):
    """Read DEM tindex; optionally add location and check existence
    
      Args:
        fname (str): DEM tindex
        path (str): directory where DEM tiles will reside
        ext (str):  file extension of DEM tiles
        exists (bool): adds column based on tile's existence

      Returns:
        (GeoDataFrame)
    """
    
    df =  gpd.read_file(fname)
    if 'location' not in df.columns:
        df['location'] = [os.path.join(path,name+ext) for name in df['name']]
    if exists:
        df['exists'] = [os.path.exists(f) for f in df['location']]
    return df


def get_tif_fragments(fname, ext='.tif'):
    """Read LAS tindex and convert it to TIF fragment index

      Args:
        fname (str): LAS tindex
        ext (str):  file extension of DEM fragments

      Returns:
        (GeoDataFrame)
    """

    df = gpd.read_file(fname)
    df['location'] = df['location'].str.replace('.las', ext, regex=False)
    return df


def main():

    scheduler = os.environ.get('DASK_SCHEDULER', 'scheduler:8786')
    client = Client(scheduler)

    path, _ = os.path.split(DEM_TINDEX)
    dems = get_target_tiles(DEM_TINDEX, path)
    tindex = get_tif_fragments(LAS_TINDEX)
    frags = client.scatter(tindex, broadcast=True)

    print('submitting tasks', flush=True)
    futures = [client.submit(merge_frags, t.location, t.srs, b[1:], frags)
        for t, b in zip(dems.itertuples(), dems.bounds.itertuples())]

    dems['processed'] = client.gather(futures)
    print('tasks gathered', flush=True)
    dems.to_file(DEM_TINDEX.replace('.gpkg', '_out.gpkg'), driver='GPKG')

    client.close()
    sys.exit(0)
    

if __name__ == '__main__':
    main()
