# -*- coding: utf-8 -*-
"""
Created on Mon Dec 24 15:45:15 2018

@author: kyy
"""

# import scipy.io.netcdf
# prog_file = scipy.io.netcdf_file('prog__0001_006.nc')
# prog_file.variables
# e_handle = prog_file.variables['e']
# print('Description =', e_handle.long_name)
# print('Shape =',e_handle.shape)
# plt.pcolormesh( e_handle[0,0] )
# plt.pcolormesh( e_handle[0,0], cmap=cm.seismic ); plt.colorbar();
# import ipywidgets
# [e_handle[:,0].min(), e_handle[:,0].max()]
# def plot_ssh(record):
#     plt.pcolormesh( e_handle[record,0], cmap=cm.spectral )
#     plt.clim(-.5,.8) # Fixed scale here
#     plt.colorbar()
#
# ipywidgets.interact(plot_ssh, record=(0,e_handle.shape[0]-1,1));
# from IPython import display
# for n in range( e_handle.shape[0]):
#     display.display(plt.gcf())
#     plt.clf()
#     plot_ssh(n)
#     display.clear_output(wait=True)

# basemap will be changed to Cartopy.
from mpl_toolkits.basemap import Basemap
import scipy.io.netcdf
from scipy.interpolate import griddata
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
#from matplotlib import cm

# import basemap as bsm
nwp_1_20_file = scipy.io.netcdf_file("E:/Data/Model/ROMS/nwp_1_20/test49/run/2008/test49_monthly_2008_02.nc")
# nwp_1_20_file.variables
lon_ = nwp_1_20_file.variables['lon_rho']
lat_ = nwp_1_20_file.variables['lat_rho']
lonu_ = nwp_1_20_file.variables['lon_u']
latu_ = nwp_1_20_file.variables['lat_u']
lonv_ = nwp_1_20_file.variables['lon_v']
latv_ = nwp_1_20_file.variables['lat_v']
sst_ = nwp_1_20_file.variables['temp']
u_ = nwp_1_20_file.variables['u']
v_ = nwp_1_20_file.variables['v']

sst_2 = np.broadcast_to(sst_[0, 39, 0:919, 0:979], (919, 979)).copy()
u_2 = np.broadcast_to(u_[0, 39, 0:919, 0:978], (919, 978)).copy()
v_2 = np.broadcast_to(v_[0, 39, 0:918, 0:979], (918, 979)).copy()

lon_3 = lon_[0:919, 0:979]
lat_3 = lat_[0:919, 0:979]
lonu_3 = lonu_[0:919, 0:978]
latu_3 = latu_[0:919, 0:978]
lonv_3 = lonv_[0:918, 0:979]
latv_3 = latv_[0:918, 0:979]
sst_2[sst_2[0:919, 0:979]> 10000.] = np.nan
u_2[u_2[0:919, 0:978]> 10000.] = np.nan
v_2[v_2[0:918, 0:979]> 10000.] = np.nan
#u_3=mlab.griddata(lonu_3.ravel(), latu_3.ravel(), u_2.ravel(), lon_3, lat_3, interp='linear')
#v_3=scipy.interpolate.griddata(lonv_3.ravel(), latv_3.ravel(), v_2.ravel(), lon_3, lat_3, interp='linear')

#sst_3[sst_3[0:919, 0:979]> 10000.] = np.nan

#np.place(sst_2, sst_2>10000., np.nan)
print('Description =', sst_.long_name)
print('Shape =', sst_.shape)

# plt.pcolormesh(lon_2, lat_2, sst_2)
# plt.colorbar()
# plt.show()


# new figure window

#m = Basemap(width=12000000, height=9000000, projection='lcc',
            # resolution=None, lat_1=45., lat_2=55, lat_0=50, lon_0=-107.)

m1 = Basemap(llcrnrlon=np.nanmin(lon_3), llcrnrlat=np.nanmin(lat_3), \
            urcrnrlon=np.nanmax(lon_3), urcrnrlat=np.nanmax(lat_3), resolution='c', \
              projection='mill')
# m.etopo()

# Given the projection, estimate plot coordinates from lats and lons
x, y = m1(lon_3, lat_3)

# m = Basemap(resolution=None, projection='mill')

#Plot the field using the fast pcolormesh routine and set the colormap to jet.
cs=m1.pcolormesh(x, y, sst_2, shading = 'flat', cmap = plt.cm.jet)
#cs=m1.pcolormesh(x, y, sst_2)



#Add a coastline and axis values.
m1.drawcoastlines(linewidth=0.25)
m1.drawcountries(linewidth=0.25)
m1.fillcontinents(color='gray',lake_color='aqua')
m1.drawmapboundary()
m1.drawparallels(np.arange(np.round(np.nanmin(lat_3)), np.round(np.nanmax(lat_3)), 10.), \
      labels=[1,0,0,0])
m1.drawmeridians(np.arange(np.round(np.nanmin(lon_3)), np.round(np.nanmax(lon_3)), 10.), \
      labels=[0,0,0,1])

#Add a colorbar and title, and then show the plot.
plt.title('nwp_1_20 SST, northwest pacific')
m1.colorbar(cs)
plt.show()
#plt.ion()
