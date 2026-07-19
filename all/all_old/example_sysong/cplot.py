#!/usr/bin/env python
# coding: utf-8

## Modules for Calculate netCDF 
import numpy as np
import pandas as pd
import xarray as xr
import sacpy as scp

## Modules for plottings
from matplotlib import gridspec
import matplotlib.pyplot as plt
import mpl_toolkits.basemap as bm
import matplotlib.colors as mcolors

def cmap_white_center(colormap, c_range=12):
    cmap = colormap
    cmap_white = [cmap(i) for i in np.arange(cmap.N)]
    cen1 = int(cmap.N / 2); cen2 = int(cen1 - 1)
    
    for i in np.arange(cen2 - (c_range-1), cen1 + (c_range)):
        cmap_white[i] = (1.0, 1.0, 1.0, 1.0)
    cmap_white = mcolors.LinearSegmentedColormap.from_list('my_cmap', cmap_white, cmap.N)
    return cmap_white

def quiver_map(udata, vdata, domain, step=2, sclf=360, widf=0.006, f_size=10, xset=1, yset=1):
    proj = bm.Basemap(projection='cyl', lon_0=(domain[3]+domain[4])/2.0, lat_0=(domain[0]+domain[1])/2.0,
              llcrnrlon=domain[3], llcrnrlat=domain[0], urcrnrlon=domain[4], urcrnrlat=domain[1], resolution='c')
    proj.drawparallels(np.arange(domain[0], domain[1]+1, domain[2]), labels=[True, False, False, False], color='none', fontsize=f_size, xoffset=xset)
    proj.drawmeridians(np.arange(domain[3], domain[4]+1, domain[5]), labels=[False, False, False, True], color='none', fontsize=f_size, yoffset=yset)
    
    lon2d, lat2d = np.meshgrid(udata.lon, udata.lat); lonproj, latproj = proj(lon2d, lat2d)
    ax.quiver(lonproj[::step,::step], latproj[::step,::step], udata[::step,::step], vdata[::step,::step], scale=sclf, width=widf)
    return lonproj, latproj

def polar_map(data, levs, domain, cmap, f_size=10, xset=1, yset=1):
    proj = bm.Basemap(projection='npstere',boundinglat=domain[0], lon_0=(domain[3]+domain[4])/2, resolution='l')
    proj.drawparallels(np.arange(domain[0], domain[1]+1, domain[2]), color='none', fontsize=f_size, xoffset=xset)
    proj.drawmeridians(np.arange(domain[3], domain[4]+1, domain[5]), color='none', fontsize=f_size, yoffset=yset)

    lon2d, lat2d = np.meshgrid(data.lon, data.lat); lonproj, latproj = proj(lon2d, lat2d)
    cf = plt.contourf(lonproj, latproj, data, levs, cmap=cmap, extend='both')
    proj.drawcoastlines(linewidth=0.5)
    proj.fillcontinents(color='lightgrey', lake_color='none')

    return cf, lonproj, latproj

### Function for ploting contourf map
def contour_map(data, levs, domain, cmap, f_size=10, xset=1, yset=1):
    proj = bm.Basemap(projection='cyl', lon_0=(domain[3]+domain[4])/2.0, lat_0=(domain[0]+domain[1])/2.0,
              llcrnrlon=domain[3], llcrnrlat=domain[0], urcrnrlon=domain[4], urcrnrlat=domain[1], resolution='c')
    proj.drawparallels(np.arange(domain[0], domain[1]+1, domain[2]), labels=[True, False, False, False], color='none', fontsize=f_size, xoffset=xset)
    proj.drawmeridians(np.arange(domain[3], domain[4]+1, domain[5]), labels=[False, False, False, True], color='none', fontsize=f_size, yoffset=yset)
    
    lon2d, lat2d = np.meshgrid(data.lon, data.lat); lonproj, latproj = proj(lon2d, lat2d)
    cf = plt.contourf(lonproj, latproj, data, levs, cmap=cmap, extend='both')
    proj.drawcoastlines(linewidth=0.5)
    proj.fillcontinents(color='lightgrey', lake_color='none')
    return cf, lonproj, latproj

def contour_map2(data, levs, domain, cmap, f_size=10, xset=1, yset=1):
    proj = bm.Basemap(projection='cyl', lon_0=(domain[3]+domain[4])/2.0, lat_0=(domain[0]+domain[1])/2.0,
              llcrnrlon=domain[3], llcrnrlat=domain[0], urcrnrlon=domain[4], urcrnrlat=domain[1], resolution='c')
    proj.drawparallels(np.arange(domain[0], domain[1]+1, domain[2]), labels=[True, False, False, False], color='none', fontsize=f_size, xoffset=xset)
    proj.drawmeridians(np.arange(domain[3], domain[4]+1, domain[5]), labels=[False, False, False, True], color='none', fontsize=f_size, yoffset=yset)
    proj.drawcoastlines(linewidth=0.5)
    proj.fillcontinents(color='lightgrey', lake_color='none')
    
    lon2d, lat2d = np.meshgrid(data.lon, data.lat); lonproj, latproj = proj(lon2d, lat2d)
    cf = plt.contourf(lonproj, latproj, data, levs, cmap=cmap, extend='both')

    return cf, lonproj, latproj

def sig_map(lonproj, latproj, sig, siglev, hatch='///', color='dimgrey'):
    sig = plt.contourf(lonproj, latproj, sig, levels=[0., siglev, 1.0], colors='none')
    for i, contour in enumerate(sig.collections):
        if i == 0:  # Only apply hatches to the first set of contours (significant ones)
            contour.set_hatch(hatch)
            contour.set_edgecolor(color)
    for contour in sig.collections:
        contour.set_linewidth(0.0)

def npolar_map(data, levs, domain, cmap, f_size=10, xset=1, yset=1):
    proj = bm.Basemap(projection='npstere', lon_0=0, boundinglat=20)
    lon2d, lat2d = np.meshgrid(data.lon, data.lat); lonproj, latproj = proj(lon2d, lat2d)
    cf = plt.contourf(lonproj, latproj, data, levs, cmap=cmap, extend='both')
    proj.drawcoastlines(linewidth=0.5)
    return cf, lonproj, latproj
