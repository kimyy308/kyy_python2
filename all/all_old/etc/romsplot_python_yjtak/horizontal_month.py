# -*- coding: utf-8 -*-
"""
Created on Fri Jan 18 10:14:24 2019

@author: Tak
"""
from mpl_toolkits.basemap import Basemap
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
#import scipy.io.netcdf
from netCDF4 import Dataset
import uv_vec2rho as uv

# Plot graph in a new window, not in a command 
# type following codes in the command 
#-- %matplotlib qt5
#-- matplotlib.pyplot.ion()


grdname='g:/auto_fennel/grid/roms_grd_auto_rdrg2_new4_smooth.nc'
grdfile = Dataset(grdname)

lon_2 = grdfile.variables['lon_rho'][:]
lat_2 = grdfile.variables['lat_rho'][:]
mask_rho = grdfile.variables['mask_rho'][:]
angle=grdfile.variables['angle'][:]

year=2010 
st_mon=1
end_mon=12
layer=1
plot_quiver=0
variable='salt'
dom_range='whole'  #whole , KODC

in_path='./spinup_2009/monthly_mean_rdrg2_39_'+str(year)+'/'
out_path='./spinup_2009/monthly_mean_rdrg2_39_'+str(year)+'/'

temp_levs=np.arange(0,30,step=2)
temp_lim=[0,30]
salt_levs=np.arange(28,35,step=1)
salt_lim=[28,35]
NO3_levs=np.arange(0,15,step=2)
NO3_lim=[0,12]
LD_levs=np.arange(0,5,step=.5)
LD_lim=[0,5]
SD_levs=np.arange(0,15,step=2)
SD_lim=[0,12]
chla_levs=np.arange(0,7,step=1)
chla_lim=[0,6]

if dom_range=='KODC':
    dic_dom={'domname':'KODC','domrange':[31.5, 37, 124, 130]}
elif dom_range=='whole':
    dic_dom={'domname':'whole','domrange':[np.nanmin(lat_2),np.nanmax(lat_2),np.nanmin(lon_2),np.nanmax(lon_2)]}
    
domaxis=dic_dom['domrange']

if variable=='temp':
    dic_var={'varname':'temp','clevs':temp_levs,'caxis':temp_lim,'varlabel':'Temp. ($^o$C)'}
elif variable=='salt':
    dic_var={'varname':'salt','clevs':salt_levs,'caxis':salt_lim,'varlabel':'Salt. (psu)'}
elif variable=='NO3':
    dic_var={'varname':'NO3','clevs':NO3_levs,'caxis':NO3_lim,'varlabel':'NO3 (μM)'}
elif variable=='LdetN':
    dic_var={'varname':'LdetritusN','clevs':LD_levs,'caxis':LD_lim,'varlabel':'LdetN (μM)'}    
elif variable=='SdetN':
    dic_var={'varname':'SdetritusN','clevs':SD_levs,'caxis':SD_lim,'varlabel':'SdetN (μM)'}        
elif variable=='chla':
    dic_var={'varname':'chlorophyll','clevs':chla_levs,'caxis':chla_lim,'varlabel':'Chla (μg/L)'}    

for i in range(st_mon,end_mon+1):
    if i < 10:
        filename=in_path+'spinup1_monthly_'+str(year)+'_0'+str(i)+'.nc'
    else:
        filename=in_path+'spinup1_monthly_'+str(year)+'_'+str(i)+'.nc'
    print(filename)
    ncfile = Dataset(filename)
    var=ncfile.variables[dic_var['varname']][:]
    var=np.squeeze(var[:])
    var=np.squeeze(var[layer-1,:,:])
    if plot_quiver==1:
        U_=ncfile.variables['u']
        U=np.squeeze(U_[:])
        U=np.squeeze(U[layer-1,:,:])
        V_=ncfile.variables['v']
        V=np.squeeze(V_[:])
        V=np.squeeze(V[layer-1,:,:])
        [uu_T,vv_T,lon_T,lat_T,mask_T]=uv.uv_vec2rho(U,V,lon_2,lat_2,angle,mask_rho,5);
    m1 = Basemap(llcrnrlon=domaxis[2], llcrnrlat=domaxis[0], \
            urcrnrlon=domaxis[3], urcrnrlat=domaxis[1], resolution='h',
             projection='mill')
 
# compute native map projection coordinates of lat/lon grid.    
    x, y = m1(lon_2, lat_2)

# m = Basemap(resolution=None, projection='mill')
# llcrnrlat,llcrnrlon,urcrnrlat,urcrnrlon
# are the lat/lon values of the lower left and upper right corners
# of the map.
# lat_ts is the latitude of true scale.
# resolution = 'c' means use crude resolution coastlines.

#Plot the field using the fast pcolormesh routine and set the colormap to jet.
    plt.figure(figsize=(7,7))
    cs=m1.pcolormesh(x,y, var, shading='flat', cmap=plt.cm.jet)
    plt.clim(dic_var['caxis'])
    cs2=m1.contour(x,y,var,dic_var['clevs'],linewidth=1,colors='k')
    cs2.clabel(inline=1,fontsize=15,fmt='%g')
#Plot quiver
    if plot_quiver==1:
        Q = m1.quiver(lon_T, lat_T, uu_T, vv_T, latlon=True,scale=1, scale_units='inches',headwidth=5,headlength=5)
        qk=plt.quiverkey(Q,0.9, 1.05 ,0.5,'0.5 $m s^{-1}$',labelpos='E')
#Add a coastline and axis values.
    m1.drawcoastlines(linewidth=.5)
    m1.drawcountries(linewidth=0.25)
    m1.fillcontinents(color=(0.8, 0.8, 0.6),lake_color=(.8,.8,.6))
    m1.drawmapboundary()
    m1.drawparallels(np.arange(np.round(np.nanmin(lat_2)), np.round(np.nanmax(lat_2)), 2.), \
      labels=[1,0,0,0],fontsize=15)
    m1.drawmeridians(np.arange(np.round(np.nanmin(lon_2)), np.round(np.nanmax(lon_2)), 2.), \
      labels=[0,0,0,1],fontsize=15)
# whole domain
    if dom_range=='whole':
        plt.annotate(dic_var['varname'],xy=(0,420),xycoords='axes pixels',fontsize=17)
        plt.annotate(str(year)+'_'+str(i),xy=(0,400),xycoords='axes pixels',fontsize=17)
# KODC domain
    elif dom_range=='KODC':
        plt.annotate(dic_var['varname'],xy=(220,360),xycoords='axes pixels',fontsize=17)
        plt.annotate(str(year)+'_'+str(i),xy=(220,340),xycoords='axes pixels',fontsize=17)
# add colorbar
    cbar = m1.colorbar(cs,location='right',pad="5%")  
    cbar.ax.tick_params(labelsize=15)
    cbar.set_label(dic_var['varlabel'],fontsize=15)

    plt.show()
    if i<10:
        savename=out_path+'monthly_'+dic_dom['domname']+'_'+dic_var['varname']+str(year)+'_0'+str(i)+'.png'
    else:
        savename=out_path+'monthly_'+dic_dom['domname']+'_'+dic_var['varname']+str(year)+'_'+str(i)+'.png'
    plt.savefig(savename)

 


