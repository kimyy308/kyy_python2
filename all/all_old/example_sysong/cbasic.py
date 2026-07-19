#!/usr/bin/env python
# coding: utf-8

## Modules for Calculate netCDF 
import numpy    as np
import xarray   as xr
import pandas   as pd
import sacpy    as scp
import netCDF4 

## Modules for caculating statistics
from scipy   import stats, signal
from sklearn import linear_model
from eofs.standard import Eof

## Modules for plottings
import matplotlib.pyplot as plt 
import matplotlib.colorbar as cb

EARTH_RADIUS = 6371000.0  # m

def norm(data):
    norm = (data - data.mean(dim='time'))/data.std(dim='time')
    return norm

def std(data):
    std = (data)/data.std(dim='time')
    return std

def season(data, months):
    return scp.XrTools.spec_moth_yrmean(data, months)

### Function for calculating correaltion & p-value with nan values
def calc_corr_pval_nan(x_array,y_array):
    ma_x_array = np.ma.masked_invalid(x_array)
    ma_y_array = np.ma.masked_invalid(y_array)
    msk_x_y    = (~ma_x_array.mask & ~ma_y_array.mask)

    corr, pval  = stats.pearsonr(ma_x_array[msk_x_y],ma_y_array[msk_x_y])

    return corr, pval

## Function for calculating anomalies
def calc_anomaly(var):
    clim_var = var.groupby("time.month").mean()
    anom_var = var.groupby("time.month") - clim_var
    
    return clim_var, anom_var

## Function for calculating anomalies with different climatology
def calc_anomaly_dclim(var1, var2):
    clim_var = var1.groupby("time.month").mean()
    anom_var = var2.groupby("time.month") - clim_var
    
    return clim_var, anom_var

## Function for extracting climate indexes in specified regions
def extract_index(var,latS,latN,lonL,lonR):

    var_sel = var.sel(lat=slice(latS,latN),lon=slice(lonL,lonR))
    lat = var_sel['lat']; lon = var_sel['lon']

    ## Calcuate the weighted mean of variable
    weights_lat      = np.cos(np.deg2rad(lat))
    weights_lat.name = "weights"  

    index_var = var_sel.weighted(weights_lat).mean(("lat","lon"))
    
    return index_var

## Function for extracting climate indexes in specified regions
def extract_index2(var,latS,latN,lonL,lonR):

    var_sel = var.sel(latitude=slice(latS,latN),longitude=slice(lonL,lonR))
    lat = var_sel['latitude']; lon = var_sel['longitude']

    ## Calcuate the weighted mean of variable
    weights_lat      = np.cos(np.deg2rad(lat))
    weights_lat.name = "weights"  

    index_var = var_sel.weighted(weights_lat).mean(("latitude","longitude"))
    
    return index_var



## Function for extracting climate indexes in specified regions
def extract_index_nan(var,latS,latN,lonL,lonR):

    var_sel = var.sel(lat=slice(latS,latN),lon=slice(lonL,lonR))
    lat = var_sel['lat']; lon = var_sel['lon']

    ## Calcuate the weighted mean of variable
    weights_lat      = np.cos(np.deg2rad(lat))
    weights_lat.name = "weights"

    index_var = var_sel.weighted(weights_lat).mean(("lat","lon"), skipna=True)
    
    return index_var


## Function for extracting climate indexes in specified regions
def extract_index_nan2(var,latS,latN,lonL,lonR):

    var_sel = var.sel(latitude=slice(latS,latN),longitude=slice(lonL,lonR))
    lat = var_sel['latitude']; lon = var_sel['longitude']

    ## Calcuate the weighted mean of variable
    weights_lat      = np.cos(np.deg2rad(lat))
    weights_lat.name = "weights"

    index_var = var_sel.weighted(weights_lat).mean(("latitude","longitude"), skipna=True)
    
    return index_var



## Functions for detrending 
def detrend_dim(da, dim, deg=1):
    # detrend along a single dimension
    p = da.polyfit(dim=dim, deg=deg)
    fit = xr.polyval(da[dim], p.polyfit_coefficients)
    return da - fit

def detrend(da, dims, deg=1):
    # detrend along multiple dimensions
    # only valid for linear detrending (deg=1)
    da_detrended = da
    da_detrended = detrend_dim(da_detrended, dims, deg=deg)
    return da_detrended

def detr(da):
    da_detrended = da
    da_detrended = detrend_dim(da_detrended, 'time', deg=1)
    return da_detrended

def moving_corr(year, index1, index2, n) :
    moving_corr = []; mv = int((n-1)/2)
    for i in range(len(year)-(n-1)):
        moving_corr.append(np.corrcoef(index1[i:i+n], index2[i:i+n])[0][1])
    moving_corr = xr.DataArray(moving_corr, dims=['time'], coords={'time':year[mv:len(year)-mv]})
    return moving_corr

#https://geocat-examples.readthedocs.io/en/latest/gallery/XY/NCL_corel_1.html
def LeadLagCorr(A, B, nlags):
    """Computes lead lag correlation to compare two series.
    # A leads B with nlags

    Parameters
    ----------
    A : array_like
        An array containing multiple variables and observations.
    B : array_like
        An array containing multiple variables and observations.
    nlags : int, optional
        The number of lag values. The default is 30.

    Returns
    -------
    coefs : array_like
        An array of size nlags containing the correlation coefficient of each
        lag at each corresponding index of the array.
    """
    coefs = np.empty(nlags)
    coefs[0] = np.corrcoef(A, B)[0, 1]

    for i in range(1, nlags):
        temp_A = A[:-i]
        temp_B = B[i:]

        r = np.corrcoef(temp_A, temp_B)[0, 1]
        coefs[i] = r

    return coefs

def LeadLagCorr_monthly(A, B, month, nlags, year):
    coefs = np.empty(nlags)
    coefs[0] = np.corrcoef(A[:,month-1], B[:,month-1])[0, 1]

    for i in range(1, nlags):
        temp_A = A[:,(month-1)-i]
        temp_B = B[:,(month-1)]

        r = np.corrcoef(temp_A, temp_B)[0, 1]
        coefs[i] = r

        if (month)-1-i < 0:
            temp_A = A[0:len(year)-1,(month-1)-i]
            temp_B = B[1:len(year),(month-1)]

            r = np.corrcoef(temp_A, temp_B)[0, 1]
            coefs[i] = r

    return coefs

def LeadLagCov_monthly(A, B, month, nlags, year):
    covs = np.empty(nlags)
    covs[0] = np.cov(A[:,month-1], B[:,month-1])[0, 1]

    for i in range(1, nlags):
        temp_A = A[:,(month-1)-i]
        temp_B = B[:,(month-1)]

        r = np.cov(temp_A, temp_B)[0, 1]
        covs[i] = r

        if (month)-1-i < 0:
            temp_A = A[0:len(year)-1,(month-1)-i]
            temp_B = B[1:len(year),(month-1)]

            r = np.cov(temp_A, temp_B)[0, 1]
            covs[i] = r

    return covs

def LeadLagCorr_monthly_pval(A, B, month, nlags, year):
    coefs = np.empty(nlags); pvals = np.empty(nlags)
    coefs[0], pvals[0] = calc_corr_pval_nan(A[:,month-1], B[:,month-1])

    for i in range(1, nlags):
        temp_A = A[:,(month-1)-i]
        temp_B = B[:,(month-1)]

        r, p = calc_corr_pval_nan(temp_A, temp_B) 
        coefs[i] = r; pvals[i] = p

        if (month)-1-i < 0:
            temp_A = A[0:len(year)-1,(month-1)-i]
            temp_B = B[1:len(year),(month-1)]

            r, p = calc_corr_pval_nan(temp_A, temp_B)
            coefs[i] = r; pvals[i] = p

    return coefs, pvals

def regress(array1,array2):
    
    n_dims = array2.shape
    
    # 1-dimension case
    if ( len(n_dims) == 1):
        
        logic = np.logical_and( ~np.isnan(array1), ~np.isnan(array2) )
        
        if np.sum(logic) >= 3:
            reg, intercept, r_value, p_value, std_err = stats.linregress( array1[logic], array2[logic] )
        else:
            reg, intercept, r_value, p_value, std_err = np.nan, np.nan, np.nan, np.nan, np.nan
    
    # 2-dimension case
    if ( len(n_dims) == 2):
        
        reg = np.empty(n_dims[1])
        intercept = np.empty(n_dims[1])
        r_value = np.empty(n_dims[1])
        p_value = np.empty(n_dims[1])
        std_err = np.empty(n_dims[1])
        
        for i in range(n_dims[1]):
        
            logic = np.logical_and( ~np.isnan(array1[:]), ~np.isnan(array2[:,i]) )

            if np.sum(logic) >= 3:
                reg[i], intercept[i], r_value[i], p_value[i], std_err[i] = stats.linregress( array1[logic], array2[logic,i] )
            else:
                reg[i], intercept[i], r_value[i], p_value[i], std_err[i] = np.nan, np.nan, np.nan, np.nan, np.nan
    
    # 3-dimension case
    if ( len(n_dims) == 3):
        
        reg = np.empty([n_dims[1],n_dims[2]])
        intercept = np.empty([n_dims[1],n_dims[2]])
        r_value = np.empty([n_dims[1],n_dims[2]])
        p_value = np.empty([n_dims[1],n_dims[2]])
        std_err = np.empty([n_dims[1],n_dims[2]])
        
        for i in range(n_dims[1]):
            for j in range(n_dims[2]):
        
                logic = np.logical_and( ~np.isnan(array1[:]), ~np.isnan(array2[:,i,j]) )

                if np.sum(logic) >= 3:
                    reg[i,j], intercept[i,j], r_value[i,j], p_value[i,j], std_err[i,j] = stats.linregress( array1[logic], array2[logic,i,j] )
                else:
                    reg[i,j], intercept[i,j], r_value[i,j], p_value[i,j], std_err[i,j] = np.nan, np.nan, np.nan, np.nan, np.nan
    
    return reg, p_value



def np2xr(xr_array,np_array):
    n_dims = xr_array.shape
    tmp = np_array.shape
    
    if (n_dims == tmp):
        output = xr.DataArray( np_array, 
                               dims = xr_array.dims, 
                               coords = xr_array.coords, attrs = xr_array.attrs )
    else:
        print( 'Numpy array shape does not math with the Xarray' )
    
    return output

def bootstrap( data1, data2, sig_lev ):

    tmp_sig_lev = (1-sig_lev/100.)

    nboot = 1000
    n2_dims = data2.shape; n2 = n2_dims[0]
    n1_dims = data1.shape; n1 = n1_dims[0]
    diff = data2.mean( axis = 0 ) - data1.mean( axis = 0 )

    ny = n1_dims[1]; nx = n1_dims[2]
    boot = np.empty([nboot,ny,nx])

    n_tail = round(nboot*(0+tmp_sig_lev/2.0))
    n_head = round(nboot*(1-tmp_sig_lev/2.0))

    for i in range(nboot):
        random_id = np.random.choice( np.arange(0,n1,1), replace = True, size = n2 )
        boot[i,:,:] = data1[random_id,:,:].mean( axis = 0 )

    boot_sorted = np.sort( boot, axis = 0 )

    boot_tail = boot_sorted[n_tail,:,:]
    boot_head = boot_sorted[n_head,:,:]

    ref_value = data2.mean( axis = 0 )
    signi = np.where( (ref_value > boot_head) | (ref_value < boot_tail), diff, np.nan )

    return diff, signi
    

def find_nearest(array, value):
    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()
    return idx, array[idx]

def calc_depth(data):
    depth_top = np.zeros_like(np.array(data.depth))
    depth_bot = np.zeros_like(np.array(data.depth))
    for i in range(len(depth_top)-1):
        depth_top[i+1] = 2 * np.array(data.depth)[i] - depth_top[i]
        depth_bot[i]   = depth_top[i+1]
    depth_bot[-1] = 2 * np.array(data.depth)[-1] - depth_bot[-2]

    dz = abs(depth_top - depth_bot)
    layer_top = depth_top

    return dz, layer_top

def _guess_bounds(points, bound_position=0.5):
    """
    Guess bounds of grid cells.
    
    Simplified function from iris.coord.Coord.
    
    Parameters
    ----------
    points: numpy.array
        Array of grid points of shape (N,).
    bound_position: float, optional
        Bounds offset relative to the grid cell centre.
    Returns
    -------
    Array of shape (N, 2).
    """
    diffs = np.diff(points)
    diffs = np.insert(diffs, 0, diffs[0])
    diffs = np.append(diffs, diffs[-1])

    min_bounds = points - diffs[:-1] * bound_position
    max_bounds = points + diffs[1:] * (1 - bound_position)

    return np.array([min_bounds, max_bounds]).transpose()


def _quadrant_area(radian_lat_bounds, radian_lon_bounds, radius_of_earth):
    """
    Calculate spherical segment areas.
    Taken from SciTools iris library.
    Area weights are calculated for each lat/lon cell as:
        .. math::
            r^2 (lon_1 - lon_0) ( sin(lat_1) - sin(lat_0))
    The resulting array will have a shape of
    *(radian_lat_bounds.shape[0], radian_lon_bounds.shape[0])*
    The calculations are done at 64 bit precision and the returned array
    will be of type numpy.float64.
    Parameters
    ----------
    radian_lat_bounds: numpy.array
        Array of latitude bounds (radians) of shape (M, 2)
    radian_lon_bounds: numpy.array
        Array of longitude bounds (radians) of shape (N, 2)
    radius_of_earth: float
        Radius of the Earth (currently assumed spherical)
    Returns
    -------
    Array of grid cell areas of shape (M, N).
    """
    # ensure pairs of bounds
    if (
        radian_lat_bounds.shape[-1] != 2
        or radian_lon_bounds.shape[-1] != 2
        or radian_lat_bounds.ndim != 2
        or radian_lon_bounds.ndim != 2
    ):
        raise ValueError("Bounds must be [n,2] array")

    # fill in a new array of areas
    radius_sqr = radius_of_earth ** 2
    radian_lat_64 = radian_lat_bounds.astype(np.float64)
    radian_lon_64 = radian_lon_bounds.astype(np.float64)

    ylen = np.sin(radian_lat_64[:, 1]) - np.sin(radian_lat_64[:, 0])
    xlen = radian_lon_64[:, 1] - radian_lon_64[:, 0]
    areas = radius_sqr * np.outer(ylen, xlen)

    # we use abs because backwards bounds (min > max) give negative areas.
    return np.abs(areas)

def grid_cell_areas(lon1d, lat1d, radius=EARTH_RADIUS):
    """
    Calculate grid cell areas given 1D arrays of longitudes and latitudes
    for a planet with the given radius.
    
    Parameters
    ----------
    lon1d: numpy.array
        Array of longitude points [degrees] of shape (M,)
    lat1d: numpy.array
        Array of latitude points [degrees] of shape (M,)
    radius: float, optional
        Radius of the planet [metres] (currently assumed spherical)
    Returns
    -------
    Array of grid cell areas [metres**2] of shape (M, N).
    """
    lon_bounds_radian = np.deg2rad(_guess_bounds(lon1d))
    lat_bounds_radian = np.deg2rad(_guess_bounds(lat1d))
    area = _quadrant_area(lat_bounds_radian, lon_bounds_radian, radius)
    return area

def weighted_depth_avg(layer_top, hm, hd, temp, dz):
    up_idx, _ = find_nearest(layer_top, hm)
    if layer_top[up_idx] - hm <= 0:
        up_top_idx = up_idx
        up_bot_idx = up_idx + 1
    else:
        up_top_idx = up_idx - 1
        up_bot_idx = up_idx

    deep_idx, _ = find_nearest(layer_top, hd)
    if layer_top[deep_idx] - hd < 0:
        deep_top_idx = deep_idx
        deep_bot_idx = deep_idx + 1
    else:
        deep_top_idx = deep_idx - 1
        deep_bot_idx = deep_idx

    weights = np.hstack((
        layer_top[up_bot_idx] - hm,
        dz[(up_top_idx) + 1:(deep_top_idx)],
        hd - layer_top[deep_top_idx]
    )) / (hd - hm)
    weights_xr = xr.DataArray(weights, dims=['depth'])

    temp_davg = temp[:,(up_top_idx):(deep_top_idx)+1].weighted(weights_xr).mean('depth')
    temp_dsum = temp[:,(up_top_idx):(deep_top_idx)+1].weighted(weights_xr).sum('depth')
    ohc       = temp_dsum * 1030 * 4180 / 1e9 / grid_cell_areas(temp_dsum.lon, temp_dsum.lat)   # unit: GJ / m^2

    return temp_davg, ohc

def time_weighted_depth_avg(layer_top, hm, hd, temp, dz):
    up_idx, _ = find_nearest(layer_top, hm)
    if layer_top[up_idx] - hm <= 0:
        up_top_idx = up_idx
        up_bot_idx = up_idx + 1
    else:
        up_top_idx = up_idx - 1
        up_bot_idx = up_idx

    deep_idx, _ = find_nearest(layer_top, hd)
    if layer_top[deep_idx] - hd < 0:
        deep_top_idx = deep_idx
        deep_bot_idx = deep_idx + 1
    else:
        deep_top_idx = deep_idx - 1
        deep_bot_idx = deep_idx

    weights = np.hstack((
        layer_top[up_bot_idx] - hm,
        dz[(up_top_idx) + 1:(deep_top_idx)],
        hd - layer_top[deep_top_idx]
    )) / (hd - hm)
    weights_xr = xr.DataArray(weights, dims=['depth'])

    temp_davg = temp[(up_top_idx):(deep_top_idx)+1].weighted(weights_xr).mean('depth')
    temp_dsum = temp[(up_top_idx):(deep_top_idx)+1].weighted(weights_xr).sum('depth')

    return temp_davg

def weighted_depth_avg_1d(layer_top, hm, hd, temp, dz, lat, lon):
    up_idx, _ = find_nearest(layer_top, hm)
    if layer_top[up_idx] - hm <= 0:
        up_top_idx = up_idx
        up_bot_idx = up_idx + 1
    else:
        up_top_idx = up_idx - 1
        up_bot_idx = up_idx

    deep_idx, _ = find_nearest(layer_top, hd)
    if layer_top[deep_idx] - hd < 0:
        deep_top_idx = deep_idx
        deep_bot_idx = deep_idx + 1
    else:
        deep_top_idx = deep_idx - 1
        deep_bot_idx = deep_idx

    weights = np.hstack((
        layer_top[up_bot_idx] - hm,
        dz[(up_top_idx) + 1:(deep_top_idx)],
        hd - layer_top[deep_top_idx]
    )) / (hd - hm)
    weights_xr = xr.DataArray(weights, dims=['depth'])

    temp_davg = temp[(up_top_idx):(deep_top_idx)+1].weighted(weights_xr).mean('depth')
    temp_dsum = temp[(up_top_idx):(deep_top_idx)+1].weighted(weights_xr).sum('depth')
    ohc       = temp_dsum * 1030 * 4180 / 1e9 / grid_cell_areas(lon, lat).sum()        # unit: GJ / m^2

    return temp_davg, ohc

def calc_EOF(data, latS, latN, lonL, lonR):
    data_region = data.sel(lon=slice(lonL,lonR),lat=slice(latS,latN))

    lat_region = data_region.lat.values
    wgt_region = np.cos(np.deg2rad(lat_region))
    wgt_region = wgt_region.reshape((len(wgt_region),1))
    solver = Eof(data_region.values, weights=wgt_region)
    
    eof = solver.eofs(neofs=10, eofscaling=2)
    pc  = solver.pcs(npcs=10,   pcscaling=1)
    varfrac = solver.varianceFraction()

    eof = xr.DataArray(eof, dims=["mode","lat","lon"], coords={"lat":data_region.lat, "lon":data_region.lon})
    pc  = xr.DataArray(pc,  dims=["time","mode"], coords={"time":data_region.time}).transpose("mode","time")
    
    return eof, pc, varfrac
