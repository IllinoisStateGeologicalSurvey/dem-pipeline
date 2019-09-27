import sys
import whitebox

wbt = whitebox.WhiteboxTools()

wbt.work_dir = '/mnt/nrcs/isgs/lidar/mclean/filtered'
print(sys.argv[1])
wbt.lidar_info(sys.argv[1], output='info.html', vlr=True, geokeys=True)
