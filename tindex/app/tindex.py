import os
import sys
import subprocess
from glob import glob
from dask.distributed import Client

def find_las(path):
    if os.path.isfile(path):
        path,_ = os.path.split(path)
    return glob(os.path.join(path,'*/*.las'))

def tindex(path):

    if os.path.isfile(path):
        path,_ = os.path.split(path)

    print "finding in %s" % path
    cmd = "find {} -name *.las".format(path)
    find = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)

    cmd = "pdal tindex --fast_boundary --lyr_name=tindex --stdin " + \
          "-f GPKG {}".format(os.path.join(path, "tindex.gpkg"))
    cmd = 'cat'
    index = subprocess.Popen(cmd.split(), stdin=find.stdout)

    return index.wait()

if __name__ == '__main__':
    os.chdir('/mnt/nrcs/isgs/lidar')
    print "pdal returned %d" % tindex(sys.argv[1])

