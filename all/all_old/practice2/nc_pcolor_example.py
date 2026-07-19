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
import numpy as np
import matplotlib.pyplot as plt
#from matplotlib import cm

# import basemap as bsm
ostia_file = scipy.io.netcdf_file("E:/Data/Observation/ostia/2009/20090101-UKMO-L4HRfnd-GLOB-v01-fv02-OSTIA.nc")
# ostia_file.variables
lon_ = ostia_file.variables['lon']
lat_ = ostia_file.variables['lat']
sst_ = ostia_file.variables['analysed_sst']
sst_2 = sst_[0, 2200:3000, 6000:7000] * 0.01
lon_2 = lon_[6000:7000]
lat_2 = lat_[2200:3000]
sst_2[sst_2 < -100] = np.nan
print('Description =', sst_.long_name)
print('Shape =', sst_.shape)

# plt.pcolormesh(lon_2, lat_2, sst_2)
# plt.colorbar()
# plt.show()

#m = Basemap(width=12000000, height=9000000, projection='lcc',
            # resolution=None, lat_1=45., lat_2=55, lat_0=50, lon_0=-107.)

m1 = Basemap(llcrnrlon=np.nanmin(lon_2), llcrnrlat=np.nanmin(lat_2), \
            urcrnrlon=np.nanmax(lon_2), urcrnrlat=np.nanmax(lat_2), resolution='c',
              projection='mill')
# m.etopo()

# Given the projection, estimate plot coordinates from lats and lons
lon_3, lat_3 = np.meshgrid(lon_2, lat_2)
x, y = m1(lon_3, lat_3)

# m = Basemap(resolution=None, projection='mill')

#Plot the field using the fast pcolormesh routine and set the colormap to jet.
cs=m1.pcolormesh(x, y, sst_2, shading='flat', cmap=plt.cm.jet)

#Add a coastline and axis values.
m1.drawcoastlines(linewidth=0.25)
m1.drawcountries(linewidth=0.25)
m1.fillcontinents(color='gray',lake_color='aqua')
m1.drawmapboundary()
m1.drawparallels(np.arange(np.round(np.nanmin(lat_2)), np.round(np.nanmax(lat_2)), 10.), \
      labels=[1,0,0,0])
m1.drawmeridians(np.arange(np.round(np.nanmin(lon_2)), np.round(np.nanmax(lon_2)), 10.), \
      labels=[0,0,0,1])

#Add a colorbar and title, and then show the plot.
plt.title('OSTIA SST, northwest pacific')
m1.colorbar(cs)
plt.show()
#plt.ion()
