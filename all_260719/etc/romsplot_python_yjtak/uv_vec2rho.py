# -*- coding: utf-8 -*-
"""
Created on Tue Jan 22 14:18:03 2019

@author: Tak
"""
import u2rho_2d as u2
import v2rho_2d as v2
import numpy as np
from pylab import *

def uv_vec2rho (u,v,lon,lat,angle,mask,skip):
    # Values at rho points
    ur=u2.u2rho_2d(u)
    vr=v2.v2rho_2d(v)  
    
    # Rotation
    cosa=np.cos(angle)
    sina=np.sin(angle)
    u=ur*cosa - vr*sina
    v=vr*cosa + ur*sina
    
    #skip
    [M,L]=lon.shape
    imin=int(floor(0.5+0.5*skip)-1)
    imax=int(floor(0.5+L-0.5*skip)-1)
    jmin=int(ceil(0.5+0.5*skip)-1)
    jmax=int(ceil(0.5+M-0.5*skip)-1)
    ured=u[jmin:jmax:skip,imin:imax:skip]
    vred=v[jmin:jmax:skip,imin:imax:skip]
    latred=lat[jmin:jmax:skip,imin:imax:skip]
    lonred=lon[jmin:jmax:skip,imin:imax:skip]
    maskred=mask[jmin:jmax:skip,imin:imax:skip]
    #  Apply mask
    ured=maskred*ured
    vred=maskred*vred
    lonred=maskred*lonred
    latred=maskred*latred
    return ured,vred,lonred,latred,maskred     