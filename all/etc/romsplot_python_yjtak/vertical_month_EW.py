# -*- coding: utf-8 -*-
"""
Created on Mon Jan 21 16:03:29 2019

@author: Tak
"""

from mpl_toolkits.basemap import Basemap
import numpy as np
import numpy.matlib as npmat
import matplotlib.pyplot as plt
from pylab import *
#import scipy.io.netcdf
from netCDF4 import Dataset
import zlevs 

# grid information
theta_s=5
theta_b=0.4
Tcline=500
N=40
Vtransform=2
Vstretching=4

# options for plotting
in_path='./spinup_2009/monthly_mean_rdrg2_38_2010/'
out_path='./spinup_2009/monthly_mean_rdrg2_38_2010/'
year=2009
st_mon=1
end_mon=12
variable='temp'
# 정선 307 - 36.925  // 308 - 36.33 // 309 - 35.855 // 310 - 35.335  // 311 -
# 34.716 // 312 - 33.975 ~ 34.0917 //313 - 33.4067 // 314 - 33
# 315 - 32.5 // 316 - 32 // 317 - 31.5
dom_range='309'

temp_levs=np.arange(0,30,step=2)
temp_lim=[0,30]
salt_levs=np.arange(25,35,step=1)
salt_lim=[25,35]
NO3_levs=np.arange(0,14,step=2)
NO3_lim=[0,12]
chla_levs=np.arange(0,7,step=0.5)
chla_lim=[0,6]


grdname='g:/auto_fennel/grid/roms_grd_auto_rdrg2_new4_smooth.nc'
grdfile = Dataset(grdname,'r')
# how to show information for a netcdf file
# grdfile.variables.keys()
# grdfile.variables['lon_rho']
lon_rho = grdfile.variables['lon_rho'][:]
lat_rho = grdfile.variables['lat_rho'][:]
mask_rho = grdfile.variables['mask_rho'][:]
angle=grdfile.variables['angle'][:]
h=grdfile.variables['h'][:]
zeta=h*0

if dom_range=='307':
    dic_dom={'domname':'Line307','domrange':[122.55, 126.3, -100, 0],'dom_lat':[36.925]}
elif dom_range=='308':
    dic_dom={'domname':'Line308','domrange':[120.72, 126.5, -100, 0],'dom_lat':[36.33]}
elif dom_range=='309':
    dic_dom={'domname':'Line309','domrange':[120.1, 127, -100, 0],'dom_lat':[36]}
elif dom_range=='310':
    dic_dom={'domname':'Line310','domrange':[119.52, 126.4, -100, 0],'dom_lat':[35.335]}
elif dom_range=='311':
    dic_dom={'domname':'Line311','domrange':[119.44, 125.9, -100, 0],'dom_lat':[34.716]}
elif dom_range=='312':
    dic_dom={'domname':'Line312','domrange':[120.37, 126, -100, 0],'dom_lat':[34]}
elif dom_range=='313':
    dic_dom={'domname':'Line313','domrange':[120.64, 126, -100, 0],'dom_lat':[33.4067]}
elif dom_range=='314':
    dic_dom={'domname':'Line314','domrange':[120.9, 128, -100, 0],'dom_lat':[33]}
elif dom_range=='315':
    dic_dom={'domname':'Line315','domrange':[121.2, 128.5, -100, 0],'dom_lat':[32.5]}    
elif dom_range=='316':
    dic_dom={'domname':'Line316','domrange':[121.8, 128.5, -100, 0],'dom_lat':[32]}    
elif dom_range=='317':
    dic_dom={'domname':'Line317','domrange':[121.9, 128.5, -100, 0],'dom_lat':[31.5]}     
elif dom_range=='318':
    dic_dom={'domname':'Line318','domrange':[123, 130, -700, 0],'dom_lat':[31]}         
dist=abs(dic_dom['dom_lat']-lat_rho[:,0])
num_stline=np.where(dist==min(dist))
num_stline=num_stline[0]

if variable=='temp':
    dic_var={'varname':'temp','clevs':temp_levs,'caxis':temp_lim,'varlabel':'Temp. ($^o$C)'}
elif variable=='salt':
    dic_var={'varname':'salt','clevs':salt_levs,'caxis':salt_lim,'varlabel':'Salt. (psu)'}
elif variable=='NO3':
    dic_var={'varname':'NO3','clevs':NO3_levs,'caxis':NO3_lim,'varlabel':'NO3 (μM)'}
elif variable=='chla':
    dic_var={'varname':'chlorophyll','clevs':chla_levs,'caxis':chla_lim,'varlabel':'Chla (μg/L)'}    

for i in range(st_mon,end_mon+1):
    if i < 10:
        filename=in_path+'spinup1_monthly_'+str(year)+'_0'+str(i)+'.nc'
    else:
        filename=in_path+'spinup1_monthly_'+str(year)+'_'+str(i)+'.nc'
    print(filename)
    ncfile = Dataset(filename)
## how to read global attributes
## for attr in ncfile.ncattrs():
#      print (attr, '=', getattr(ncfile, attr))
##
    var=ncfile.variables[dic_var['varname']][:]
    var=np.squeeze(var[0,:,num_stline,:])
    zeta=ncfile.variables['zeta'][:]
    depth=zlevs.zlevs(h,zeta,theta_s,theta_b,Tcline,N,Vtransform,Vstretching,'r')
    Yi=np.squeeze(depth[:,num_stline,:])
    var[var > 100] = NaN
#    var=np.ma.masked_where(np.isnan(var),var)
    X1=lon_rho[0,:]
    x_1=np.where(X1>=dic_dom['domrange'][0]);x_1=np.array(x_1)
    x_2=np.where(X1>=dic_dom['domrange'][1]);x_2=np.array(x_2)
    x=lon_rho[num_stline,x_1[0,0]:x_2[0,0]]
    Xi=npmat.repmat(x,40,1)
    var=var[:,x_1[0,0]:x_2[0,0]]
    Yi=Yi[:,x_1[0,0]:x_2[0,0]]
    


#Plot the field using the fast pcolormesh routine and set the colormap to jet.
    fig,ax=plt.subplots(figsize=(10,7))
    cs=plt.pcolormesh(Xi,Yi, var, shading='flat', cmap=plt.cm.jet)
    plt.clim(dic_var['caxis'])
    cs2=plt.contour(Xi,Yi,var,dic_var['clevs'],linewidth=1,colors='k')
    cs2.clabel(fontsize=15,fmt='%g',inline_spacing=-1)
    plt.xlim([dic_dom['domrange'][0],dic_dom['domrange'][1]]);plt.ylim(dic_dom['domrange'][2],dic_dom['domrange'][3])
    plt.xlabel('Longitude $^o$E',fontsize=15);plt.ylabel('Depth (m)',fontsize=15)
    plt.tick_params(labelsize=15)
#Add a coastline and axis values.
    
    plt.annotate(dic_var['varname'],xy=(0,30),xycoords='axes pixels',fontsize=17)
    plt.annotate(str(year)+'_'+str(i),xy=(0,10),xycoords='axes pixels',fontsize=17)
# add colorbar
    cbar = fig.colorbar(cs)  
    cbar.ax.tick_params(labelsize=15)
    cbar.set_label(dic_var['varlabel'],fontsize=15)

    plt.show()
    if i<10:
        savename=out_path+'monthly_'+dic_dom['domname']+'_'+dic_var['varname']+str(year)+'_0'+str(i)+'.png'
    else:
        savename=out_path+'monthly_'+dic_dom['domname']+'_'+dic_var['varname']+str(year)+'_'+str(i)+'.png'
    plt.savefig(savename)

 


