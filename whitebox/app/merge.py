import os
import sys
import time
from glob import glob
from whitebox import WhiteboxTools
from dask.distributed import Client
import geopandas as gpd

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


def mosaic(fname, flist, preserve=True, verbose=False):
    
    print(f'mosaic: {fname} from {len(flist)} frags', flush=True)
    wbt = WhiteboxTools()
    wbt.verbose = verbose

    if len(flist) < 2:
        print('mosaic: less than 2 tifs', fname, flist, flush=True)
        return False
        
    wbt.mosaic(inputs=';'.join(flist), output=fname, method='nn')
    
    if not preserve:
        for f in flist:
            try:
                os.remove(f)
            except FileNotFoundError:
                pass

    return True


def generate_quads(fname, bounds):
    basename, ext = os.path.splitext(fname)
    minx, miny, maxx, maxy = bounds
    midx, midy = minx+(maxx-minx)/2, miny+(maxy-miny)/2
    return {basename+'_sw'+ext: (minx, miny, midx, midy),
            basename+'_nw'+ext: (minx, midy, midx, maxy),
            basename+'_se'+ext: (midx, miny, maxx, midy),
            basename+'_ne'+ext: (midx, midy, maxx, maxy)}


def merge_frags(fname, bounds, frags, maxfiles=200):

    print(f'merge_frags:', fname, flush=True)

    # don't reprocess existing DEM tiles
    if os.path.exists(fname):
        print(f'{fname} exists', flush=True)
        return True

    sindex = frags.sindex
    overlap = frags.iloc[list(sindex.intersection(bounds))]

    # only proceed if all DEM frags are available
    for f in overlap['location']:
        if not os.path.exists(f):
            print(f'merge_frags: frag {f} not found for {fname}', flush=True)
            return False
        
    if len(overlap) < maxfiles:
        return mosaic(fname, overlap['location'])

    else:
        flist = []
        for qname,qbounds in generate_quads(fname, bounds).items():
            flist.append(qname)
            overlap = frags.iloc[list(sindex.intersection(qbounds))]
            mosaic(qname, overlap['location'])
        
        return mosaic(fname, flist, preserve=False)
        

def get_target_tiles(fname, path, ext='.tif', exists=False):
    """Read DEM tindex; optionally add location and check existence
    
      Args:
        fname (str): DEM tindex
        path (str): directory where DEM tiles will reside
        ext (str):  file extension of DEM tiles
        exists (bool): check for tile existence

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
        ext (str):  file extension of DEM tiles

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
    futures = [client.submit(merge_frags, fname, bounds[1:], frags)
        for fname, bounds in zip(dems['location'], dems.bounds.itertuples())]

    dems['processed'] = client.gather(futures)
    print('tasks gathered', flush=True)
    dems.to_file(DEM_TINDEX.replace('.gpkg', '_out.gpkg'), driver='GPKG')

    client.close()
    sys.exit(0)
    

if __name__ == '__main__':
    main()
