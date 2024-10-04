# -*- coding: utf-8 -*-

"""
 The name of program is 'make_CMIP6_dwonscaling_data_to_stddepth'
 that converts ROMS output monthly variables (sigma coordinate) 
 to z-coordinate variables

  It needs 
  monthly variables such as (temp, salt, zeta, u, v, w) and
  dimensions are [lon, lat, (s), t]
 
  It creates
  monthly variables such as (temp, salt, zeta, u, v, w) and
  dimensions are [lon, lat, (z), t])
 
  e-mail:kimyy308@snu.ac.kr
 
  Updated    30-Jun-2021 by Yong-Yub Kim
"""

# import os
# import conda

# conda_file_dir = conda.__file__
# conda_dir = conda_file_dir.split('lib')[0]
# proj_lib = os.path.join(os.path.join(conda_dir, 'Library', 'share'))
# os.environ["PROJ_LIB"] = proj_lib



import numpy as np
import matplotlib.pyplot as plt
# from inspect import cleandoc as dedent
# from matplotlib.cbook import dedent
# from mpl_toolkits.basemap import Basemap

try:
    import netCDF4 as netCDF4
except:
    import netCDF3 as netCDF
import pyroms
from pyroms.hgrid import *
from pyroms.vgrid import *
from pyroms.grid import *
from pyroms import io

class RCM:
    def __init__(self):
        self.result = 0
        
    def set_testnumber(self, testnumbers):
        self.testnumber = testnumbers
        
    def set_testname(self, testnames):
        self.testname = testnames
        
    def set_model(self, model):
        self.modelname = model
    
    def set_rootdir(self, rootdir):
        self.rootdir = rootdir
    
    def set_progress(self, progress):
        self.progress = progress

class flag_process:
    def __init__(self):
        self.result = 0
        
    def set_flag_save_nc(self, flag):
        self.flag_save_nc = flag
        
    def set_flag_save_mat(self, flag):
        self.flag_save_mat = flag
        
# class grid_ROMS:
#     def __init__(self):
#         self.result = 0
    
#     def set_Vstretching(self, Vstretching):
#         self.Vstretching = Vstretching
        
#     def set_Vtransform(self, Vtransform):
#         self.Vtransform = Vtransform

filename = 'D:\Data\Model\ROMS\nwp_1_20\test53\run\1976\test53_monthly_1976_01.nc'
grid = pyroms.grid(filename)

flags = flag_process()    
flags.set_flag_save_mat(1)    
flags.set_flag_save_nc(2)

# define RCM information class
all_RCM = RCM()
all_RCM.set_testnumber(['test2102', 'test2103', 'test2104', 'test2105', 'test2106'] )
all_RCM.set_testname(['RCM-CNRM', 'RCM-EC-Veg', 'RCM-ACC', 'RCM-CNRM-HR', 'RCM-CMCC'] )
all_RCM.set_model('nwp_1_20')
all_RCM.set_progress('run')

stddepth = [-10,-20,-30,-50,-75,-100, \
    -150,-200,-300,-400,-500, \
    -700,-1000,-1250,-1500, \
    -1750,-2000,-2250,-2500, \
    -3000,-3500,-4000,-4500,-5000];


for i in range(0,len(all_RCM.testnumber)):
    print(all_RCM.testnumber[i])









