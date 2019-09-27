import os
from glob import glob
from whitebox import WhiteboxTools

def main():

    las_files = '/mnt/nrcs/isgs/lidar/alexander'
    filtered = os.path.join(las_files, 'filtered')
    rasters = os.path.join(las_files, 'rasters')

    if not os.path.exists(filtered):
        os.makedirs(filtered)
    if not os.path.exists(rasters):
        os.makedirs(rasters)

    
    wbt = WhiteboxTools()
    wbt.work_dir = las_files
    os.chdir(las_files)
    tifs = glob(os.path.join(las_files, '*.tif'))
    tif_str = ';'.join(tifs)
    wbt.mosaic(inputs=tif_str, output='/app/mosaic.tif', method='nn')
    

if __name__ == '__main__':
    main()
