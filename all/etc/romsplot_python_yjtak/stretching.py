# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 13:32:53 2019

@author: Tak
"""
# STRETCHING:  Compute ROMS vertical coordinate stretching function
#
# [s,C]=stretching(Vstretching, theta_s, theta_b, hc, N, kgrid, report)
#
# Given vertical terrain-following vertical stretching parameters, this
# routine computes the vertical stretching function used in ROMS vertical
# coordinate transformation. Check the following link for details:
#
#    https://www.myroms.org/wiki/index.php/Vertical_S-coordinate
#
# On Input:
#
#    Vstretching   Vertical stretching function:
#                    Vstretching = 1,  original (Song and Haidvogel, 1994)
#                    Vstretching = 2,  A. Shchepetkin (UCLA-ROMS, 2005)
#                    Vstretching = 3,  R. Geyer BBL refinement
#                    Vstretching = 4,  A. Shchepetkin (UCLA-ROMS, 2010)
#    theta_s       S-coordinate surface control parameter (scalar)
#    theta_b       S-coordinate bottom control parameter (scalar)
#    hc            Width (m) of surface or bottom boundary layer in which
#                    higher vertical resolution is required during
#                    stretching (scalar)
#    N             Number of vertical levels (scalar)
#    kgrid         Depth grid type logical switch:
#                    kgrid = 0,        function at vertical RHO-points
#                    kgrid = 1,        function at vertical W-points
#    report        Flag to report detailed information (OPTIONAL):
#                    report = false,   do not report
#                    report = true,    report information
#
# On Output:
#
#    s             S-coordinate independent variable, [-1 <= s <= 0] at
#                    vertical RHO- or W-points (vector)
#    C             Nondimensional, monotonic, vertical stretching function,
#                    C(s), 1D array, [-1 <= C(s) <= 0]

import numpy as np
from pylab import *

def stretching(Vstretching, theta_s, theta_b, hc, N, kgrid,report):
    s=np.array([])
    C=np.array([])
    #--------------------------------------------------------------------------
    #  Set several parameters.
    #--------------------------------------------------------------------------
    
    if (Vstretching < 1) or (Vstretching > 4):
        print(' ')
        print(['*** Error:  STRETCHING - Illegal parameter Vstretching = '+str(Vstretching)])
        print(' ')
    Np=N+1;

    #--------------------------------------------------------------------------
    # Compute ROMS S-coordinates vertical stretching function
    #--------------------------------------------------------------------------

    # Original vertical stretching function (Song and Haidvogel, 1994).

    if (Vstretching == 1):
        ds=1.0/N
        if (kgrid == 1):
            Nlev=Np
            lev=np.arange(0,N+1)
            s=(lev-N)*ds
        else:
            Nlev=N
            lev=np.arange(1,N+1)-0.5
            s=(lev-N)*ds
        if (theta_s > 0):
            Ptheta=sinh(theta_s*s)/sinh(theta_s)
            Rtheta=tanh(theta_s*(s+0.5))/(2.0*tanh(0.5*theta_s))-0.5
            C=(1.0-theta_b)*Ptheta+theta_b*Rtheta
        else:
            C=s

    # A. Shchepetkin (UCLA-ROMS, 2005) vertical stretching function.

    elif (Vstretching == 2):
        alfa=1.0
        beta=1.0
        ds=1.0/N
        if (kgrid == 1):
            Nlev=Np
            lev=np.arange(0,N+1)
            s=(lev-N)*ds
        else:
            Nlev=N
            lev=np.arange(1,N+1)-0.5
            s=(lev-N)*ds
        if (theta_s > 0):
            Csur=(1.0-cosh(theta_s*s))/(cosh(theta_s)-1.0)
            if (theta_b > 0):
                Cbot=-1.0+sinh(theta_b*(s+1.0))/sinh(theta_b)
                weight=(s+1.0)^alfa*(1.0+(alfa/beta)*(1.0-(s+1.0)^beta))
                C=weight*Csur+(1.0-weight)*Cbot
            else:
                C=Csur
        else:
            C=s

    #  R. Geyer BBL vertical stretching function.

    elif (Vstretching == 3):
        ds=1.0/N
        if (kgrid == 1):
            Nlev=Np
            lev=lev=np.arange(0,N+1)
            s=(lev-N)*ds
        else:
            Nlev=N
            lev=lev=np.arange(1,N+1)-0.5
            s=(lev-N)*ds
        if (theta_s > 0):
            exp_s=theta_s      #  surface stretching exponent
            exp_b=theta_b      #  bottom  stretching exponent
            alpha=3            #  scale factor for all hyperbolic functions
            Cbot=log(cosh(alpha*(s+1)^exp_b))/log(cosh(alpha))-1
            Csur=-log(cosh(alpha*abs(s)^exp_s))/log(cosh(alpha))
            weight=(1-tanh( alpha*(s+.5)))/2
            C=weight*Cbot+(1-weight)*Csur
        else:
            C=s

    # A. Shchepetkin (UCLA-ROMS, 2010) double vertical stretching function
    # with bottom refinement

    elif (Vstretching == 4):
        ds=1.0/N
        if (kgrid == 1):
            Nlev=Np
            lev=lev=np.arange(0,N+1)
            s=(lev-N)*ds
        else:
            Nlev=N
            lev=lev=lev=np.arange(1,N+1)-0.5
            s=(lev-N)*ds
        if (theta_s > 0):
            Csur=(1.0-cosh(theta_s*s))/(cosh(theta_s)-1.0)
        else:
            Csur=-s^2
        if (theta_b > 0):
            Cbot=(exp(theta_b*Csur)-1.0)/(1.0-exp(-theta_b))
            C=Cbot
        else:
            C=Csur

    # Report S-coordinate parameters.

    if (report):
        print(' ')
        if (Vstretching == 1):
            print(['Vstretching = '+str(Vstretching)+'   Song and Haidvogel (1994)'])
        elif (Vstretching == 2):
            print(['Vstretching = '+str(Vstretching)+'   Shchepetkin (2005)'])
        elif (Vstretching == 3):
            print(['Vstretching = '+str(Vstretching)+'   Geyer (2009), BBL'])
        elif (Vstretching == 4):
            print(['Vstretching = '+str(Vstretching)+'   Shchepetkin (2010)'])
    if (kgrid == 1):
        print(['   kgrid    = '+str(kgrid)+'   at vertical W-points'])
    else:
        print(['   kgrid    = '+str(kgrid)+'   at vertical RHO-points'])
    print(['   theta_s  = '+str(theta_s)+'     '])
    print(['   theta_b  = '+str(theta_b)+'     '])
    print(['   hc       = '+str(hc)+'     '])
    print(' ')
    print(' S-coordinate curves: k, s(k), C(k)')
    print(' ')
    return s,C