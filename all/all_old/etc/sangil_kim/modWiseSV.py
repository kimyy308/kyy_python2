#!/usr/bin/env python3
# -*- coding: cp949 -*-
"""
SYNOPSIS
       The WiseSV module for Scientific Visualization

EXAMPLES
       from modWiseSV import function
"""
# Created: Jun. 2015

import os, logging
from glob import glob
from datetime import timedelta

# import WISE modules
from modWiseUT import runSCP, runProcess, symLink, copyReplace, initWiseR


class WiseSV(object): # Scientific Visualization class
   def __init__( self, config, timer, logger, webServer='WISE4' ):
      self.stime   = timer.KST
      self.log     = logger.add(config.logdir,'WiseSV',config.logLevel)
      self.datadir = os.path.join(config.datadir,'WiseSV')
      self.workdir = config.castdir
      self.castdir = os.path.join(self.workdir,'WiseSV')
      self.ipaddr  = config.get(webServer,'ipaddr')
      self.userid  = config.get(webServer,'userid')
      self.prcp_1hr = config.get(webServer,'prcp_1hr')
      self.uv10rain = config.get(webServer,'uv10rain')
      self.flist   = []

      if not os.path.isdir(self.castdir): os.makedirs(self.castdir)

      self.prcpScript = 'prcp_1hr.ncl'
      self.uvpScript  = 'uv10rain.ncl'
      self.ncl_x      = os.path.join(config.ncarg,'bin','ncl')

      # NCL needs 'aws_rain.rgb' and 'aws_temp.rgb' in colormaps directory.
      colormaps = self.datadir+':'+os.path.join(config.ncarg,'lib/ncarg/colormaps')
      self.env = {'NCARG_ROOT':config.ncarg\
                 ,'NCARG_LIB':os.path.join(config.ncarg,'lib')\
                 ,'NCARG_RANGS':os.path.join(config.ncarg,'lib/ncarg/rangs')\
                 ,'NCARG_COLORMAPS':colormaps\
                 ,'PATH'  :os.path.join(config.ncarg,'bin')}

   def __del__( self ):
      if self.log.getEffectiveLevel() != logging.DEBUG:
         for f in self.flist:
            try: os.remove(f) #; self.log.debug('delete: %s'%f)
            except: self.log.warn('deleting error: '+f+' is not exist.')

   def draw( self, wksOut='prcp_1hr', iFcstHour=6 ):
      wrfOut = glob(os.path.join(self.workdir,'WiseFC','wrfout*'))
      if len(wrfOut) != 1:
         self.log.error('There must be only one of wrfout*')
         return ''

      # Link wrfout* to wrfout.nc to determine file type by ncl
      wrfOutNC = os.path.join(self.castdir,'wrfout.nc')
      symLink(wrfOut[0],wrfOutNC,self.log)

      # Create ncl script
      replaceNCL = {\
         'wrfout':wrfOutNC\
        ,'wksout':wksOut\
        ,'datadir':self.datadir}

      etime = self.stime + timedelta(hours=iFcstHour)

      # ['time??'] include every 30 minute time from start to end in KST
      t = self.stime; n = 0
      while t <= etime:
         replaceNCL['time'+str(n).zfill(2)] = t.strftime("%Y-%m-%d_%H:%M:%S")
         t += timedelta(minutes=30); n += 1

      ifname = os.path.join(self.datadir,self.prcpScript+'.pre')
      ofname = os.path.join(self.castdir,self.prcpScript)
      nclFile = copyReplace(replaceNCL,ifname,ofname,self.log)
      self.flist.append(nclFile)

      cmd = self.ncl_x+' '+self.prcpScript
      #if runProcess(cmd,self.castdir,self.log,self.env,timeoutS=600): return ''
      runProcess(cmd,self.castdir,self.log,self.env,timeoutS=600)
      psOut = os.path.join(self.castdir,wksOut+'.ps')
      if os.path.isfile(psOut): self.flist.append(psOut)
      else: return ''

      return psOut

   def extrans( self ):
      wrfOut = glob(os.path.join(self.workdir,'WiseFC','wrfout*'))
      if len(wrfOut) != 1:
         self.log.error('There must be only one of wrfout*')
         return ''

      # Link wrfout* to wrfout.nc to determine file type by ncl
      wrfOutNC = os.path.join(self.castdir,'wrfout.nc')
      symLink(wrfOut[0],wrfOutNC,self.log)
      uvpOutNC = os.path.join(self.castdir,'uv10rain.nc')
      if os.path.isfile(uvpOutNC): os.remove(uvpOutNC)

      # Create ncl script
      replaceNCL = {\
         'wrfout':wrfOutNC\
        ,'uvpout':uvpOutNC}

      ifname = os.path.join(self.datadir,self.uvpScript+'.pre')
      ofname = os.path.join(self.castdir,self.uvpScript)
      nclFile = copyReplace(replaceNCL,ifname,ofname,self.log)
      self.flist.append(nclFile)

      cmd = self.ncl_x+' '+self.uvpScript
      #if runProcess(cmd,self.castdir,self.log,self.env,timeoutS=600): return ''
      runProcess(cmd,self.castdir,self.log,self.env,timeoutS=600)
      if os.path.isfile(uvpOutNC): self.flist.append(uvpOutNC)
      else: return ''

      return uvpOutNC

   def run( self ):

      uvpOutNC = self.extrans()
      if uvpOutNC:
         ncBasename = self.stime.strftime("uv10rain_%Y%m%d%H%MK.nc")
         # Transper 'uv10rain.nc' to Wise Web (wise4)
         webOut = self.userid+'@'+self.ipaddr+':'+self.uv10rain+'/'+ncBasename
         runSCP(uvpOutNC,webOut,self.castdir,self.log,60)
      #else: return True

      psOut = self.draw()
      if psOut:
         # Convert *.ps to *.gif
         gifBasename = self.stime.strftime("%Y%m%d%H%M.gif")
         convert = 'umask 022; '\
           +"/usr/bin/convert -trim -density 100 -delay 100 -loop 0 %s %s"%(psOut,gifBasename)
         runProcess(convert,self.castdir,self.log,timeoutS=600)

         # Transper *.gif to Wise Web (wise4)
         webOut = self.userid+'@'+self.ipaddr+':'+self.prcp_1hr+'/'+gifBasename
         runSCP(gifBasename,webOut,self.castdir,self.log,60)
      else: return True

      return False

if __name__ == '__main__':
   ctime = '201506121200' # UTC
   config,timer,logger = initWiseR('../.wiserc','D','WiseSV',ctime,60)

   WiseSV(config,timer,logger).run()

