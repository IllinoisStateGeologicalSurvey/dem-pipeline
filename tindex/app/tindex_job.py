import os
import sys
import csv
import subprocess
from dask.distributed import Client


def get_counties(path):
    return [os.path.join(path, c) for c in os.listdir(path) 
            if os.path.isdir(os.path.join(path, c))]


def build_tindex(path):

    if os.path.isfile(path):
        path,_ = os.path.split(path)

    cmd = "find {} -name *.las".format(path)
    find = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)

    cmd = "pdal tindex --fast_boundary --lyr_name=tindex --stdin " + \
          "-f GPKG {}".format(os.path.join(path, "tindex.gpkg"))
    tindex = subprocess.Popen(cmd.split(), stdin=find.stdout)
    return tindex.wait()


def create_tindex(path):

    if not os.path.exists(os.path.join(path, 'tindex.gpkg')):
        if build_tindex(path) == 0:
            return "succeess"
        else:
            return "fail"
    else:
        return "exists"

def write_status(fname, counties, status):

    with open(fname, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(zip(counties, status))


def main():

    lidar_path = os.environ.get('LIDAR_PATH', '/mnt/nrcs/isgs/lidar')
    scheduler = os.environ.get('DASK_SCHEDULER', 'scheduler:8786')

    counties = get_counties(lidar_path)

    client = Client(scheduler)

    s = client.map(create_tindex, counties)
    status = client.gather(s)

    fname = os.path.join(lidar_path, 'tindex_update.csv')
    write_status(fname, counties, status)
    sys.exit(0)
    

if __name__ == '__main__':
    main()

