C     -*- fortran -*-
C     This file is autogenerated with f2py (version:1.21.0)
C     It contains Fortran 77 wrappers to fortran functions.

      subroutine f2pywraptry_range (try_rangef2pywrap, lbi, ubi, l
     &bj, ubj, xgrd, ygrd, imin, imax, jmin, jmax, xo, yo)
      external try_range
      integer lbi
      integer ubi
      integer lbj
      integer ubj
      integer imin
      integer imax
      integer jmin
      integer jmax
      real*8 xo
      real*8 yo
      real*8 xgrd(ubi-lbi+1,ubj-lbj+1)
      real*8 ygrd(ubi-lbi+1,ubj-lbj+1)
      logical try_rangef2pywrap, try_range
      try_rangef2pywrap = .not.(.not.try_range(lbi, ubi, lbj, ubj,
     & xgrd, ygrd, imin, imax, jmin, jmax, xo, yo))
      end


      subroutine f2pywrapinside (insidef2pywrap, nb, xb, yb, xo, y
     &o)
      external inside
      integer nb
      real*8 xo
      real*8 yo
      real*8 xb(nb + 1)
      real*8 yb(nb + 1)
      logical insidef2pywrap, inside
      insidef2pywrap = .not.(.not.inside(nb, xb, yb, xo, yo))
      end
