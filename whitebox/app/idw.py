import os
import sys
from glob import glob
from whitebox import WhiteboxTools
from dask.distributed import Client, as_completed

LAS_DIRS = '/mnt/nrcs/isgs/lidar/LAS_dirs.txt'


def all_processed(path):
    las = glob(os.path.join(path, '*.las'))
    tif = glob(os.path.join(path, '*.tif'))
    for l in las:
        if l.replace('.las', '.tif') not in tif:
            return False
    return True


def idw_job(path):

    if all_processed(path):
        return f'{path} processed'

    wbt = WhiteboxTools()
    wbt.verbose = False
    wbt.work_dir = path

    wbt.lidar_idw_interpolation(parameter='elevation', returns='last', 
            resolution=10, weight=1.0, radius=20.0, 
            exclude_cls='3,4,5,6,7,18')

    return f'{path} complete'


def main():

    scheduler = os.environ.get('DASK_SCHEDULER', 'scheduler:8786')
    client = Client(scheduler)

    # start N tasks and add new one as the finish
    with open(LAS_DIRS) as f:
        futures = [client.submit(idw_job, f.readline().rstrip()) 
                   for i in range(12)]
        seq = as_completed(futures)
        for future in seq:
            print(future.result())
            del future
            seq.add(client.submit(idw_job, f.readline().rstrip()))

    sys.exit(0)


if __name__ == '__main__':
    main()
