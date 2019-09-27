import os
import whitebox

wbt = whitebox.WhiteboxTools()

lasdir = '/mnt/nrcs/isgs/lidar/mclean/LAS'
filtered = '/mnt/nrcs/isgs/lidar/mclean/filtered'

for fname in os.listdir(lasdir)[:10]:
    infile = os.path.join(lasdir, fname)
    outfile = os.path.join(filtered, fname)
    wbt.lidar_ground_point_filter(infile, outfile, 
        radius=10.0, slope_threshold=45.0, height_threshold=10.0,
        classify=True, slope_norm=False)

