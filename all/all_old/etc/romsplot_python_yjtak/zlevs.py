# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 13:27:59 2019

@author: Tak
"""
#  this function compute the depth of rho or w points for ROMS
#
#  On Input:
#
#    type    'r': rho point 'w': w point 
#
#  On Output:
#
#    z       Depths (m) of RHO- or W-points (3D matrix).

import numpy as np
from pylab import *
import stretching as Vstr

def zlevs(h,zeta,theta_s,theta_b,hc,N,Vtransform,Vstretching,ztype):
    if type=='w':
        kgrid=1
    else:
        kgrid=0
    [sc,Cs]=Vstr.stretching(Vstretching, theta_s, theta_b, hc, N, kgrid,'true')
        
    hr=h
    zetar=zeta
    Np=N+1
    [X,Y]=h.shape
    #--------------------------------------------------------------------------
    # Compute depths (m) at requested C-grid location.
    #--------------------------------------------------------------------------
    z=np.zeros((N,X,Y))*NaN
    if (Vtransform == 1):
        for k in range(0,N):
            z0=(sc[k]-Cs[k])*hc + Cs[k]*hr
            z[k,:,:]=z0 + zetar*(1.0 + z0/hr)
        if ztype=='w':
            z=np.zeros((N+1,X,Y))*NaN
            z[1,:,:]=-hr
            for k in range(1,Np):
                z0=(sc[k]-Cs[k])*hc + Cs[k]*hr
                z[k,:,:]=z0 + zetar*(1.0 + z0/hr)
    elif (Vtransform == 2):
        for k in range(0,N):
            z0=(hc*sc[k]+Cs[k]*hr)/(hc+hr)
            z[k,:,:]=zetar+(zeta+hr)*z0
        if ztype=='w':
            z=np.zeros((N+1,X,Y))*NaN
            for k in range(0,Np):
                z0=(hc*sc[k]+Cs[k]*hr)/(hc+hr)
                z[k,:,:]=zetar+(zetar+hr)*z0
    return z

