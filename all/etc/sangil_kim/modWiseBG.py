#!/usr/bin/env python3
# -*- coding: cp949 -*-
"""
SYNOPSIS
   ##############################################################################
   # SUF5_LC05_MKBG run(1 hour interval)
   ##############################################################################
   # [1.0] suf5_lc05_mkbg_prep_usst.ksh           : USST
   # [2.0] suf5_lc05_mkbg_prep_mgrd.ksh           : METGRID
   # [3.0] suf5_lc05_mkbg_init_latb.ksh           : MAKE INIT & BOUNDARY CONDITION
   # [4.0] suf5_lc05_mkbg_fcst.ksh                : RUN MAIN FORECAST
   # [5.0] suf5_lc05_mkbg_lfmp.ksh                : RUN P-LEVEL INTERPOLATION
   ##############################################################################
   # SETUP FOR SUF5 LC05 MKBG
   ##############################################################################

EXAMPLES
       from modWiseBG import function
"""
# Created: May. 2015

import os, sys, datetime, logging
from time import sleep
from glob import glob
from itertools import product
from string import ascii_uppercase
from multiprocessing import Process

# import WISE modules
from modWiseUT import runProcess,queSub,symLink,copyReplace


class WiseBG(object): # make background field
   def __init__( self, config, timer, logger ):
      self.supercom = config.get('OPERATION','supercom')
      self.prepost  = config.get('OPERATION','prepost')
      self.quename  = config.get('OPERATION','quename')
      self.stime    = timer.UTC;    self.tz = 'UTC'
      self.bgIntervalM  =timer.bgIntervalM # the B.C. data interval(min) from KLAPS's KLBG
      self.histIntervalM=timer.histIntervalM # output data interval(min) for SF01
      self.log      = logger.add(config.logdir,'WiseBG',config.logLevel)
      self.datadir  = os.path.join(config.datadir,'WiseR5k')
      self.workdir  = config.castdir
      self.castdir  = os.path.join(self.workdir,'WiseBG')
      self.wrfprd   = os.path.join(self.workdir,'wrfprd')
      self.gribdir  = os.path.join(self.wrfprd,'d01/grib')
      self.netcdf   = config.netcdf
      self.flist    = []

      if not os.path.isdir(self.castdir): os.makedirs(self.castdir)
      #if not os.path.isdir(self.wrfprd): os.makedirs(self.wrfprd)
      if not os.path.isdir(self.gribdir): os.makedirs(self.gribdir)

      self.prepMetGrid_x = os.path.join(config.bindir,'wr5k_mkbg_prep_mgrd.x')
      self.prepUngrib_x  = os.path.join(config.bindir,'wr5k_prep_ugrb.x')
      self.initLatB_x    = os.path.join(config.bindir,'wr5k_mkbg_init_latb.x')
      self.runForecast_x = os.path.join(config.bindir,'wr5k_mkbg_run_fcst.x')
      self.lfmpost_x     = os.path.join(config.bindir,'lfmpost_wrfv3.exe')

      self.KLBGDAOU = config.get('KLBG','KLBGDAOU')
      self.KLFSDAIN = config.get('KLFS','KLFSDAIN')

      # Link static directory for BG
      srcF = os.path.join(self.datadir,'static')
      dstF = os.path.join(self.workdir,'static')
      symLink(srcF,dstF,self.log)

      self.dECMWF=os.path.join(config.datadir,'ECMWF/read_nc/OUT/5yr_tendays')
      self.ECMWF    = ( 'SM1','SM2','SM3','SM4'\
                       ,'ST1','ST2','ST3','ST4')

      self.WiseR5k  = { 'geo_em.d01.nc':'geo_em.d01-MKBG.nc'\
                       ,'geo_em.d02.nc':'geo_em.d02-MKBG.nc'\
                       ,'METGRID.TBL'  :'METGRID.TBL.ARW'\
                       ,'RRTM_DATA'    :'RRTM_DATA'\
                       ,'GENPARM.TBL'  :'GENPARM.TBL'\
                       ,'LANDUSE.TBL'  :'LANDUSE.TBL'\
                       ,'SOILPARM.TBL' :'SOILPARM.TBL'\
                       ,'VEGPARM.TBL'  :'VEGPARM.TBL'} # static files
      for d,s in self.WiseR5k.items():
         srcF = os.path.join(self.datadir,s)
         dstF = os.path.join(self.castdir,d)
         symLink(srcF,dstF,self.log) #; self.flist.append(dstF)

   def __del__( self ):
      if self.log.getEffectiveLevel() != logging.DEBUG:
         for f in self.flist:
            try: os.remove(f) #; self.log.debug('delete: %s'%f)
            except: self.log.warn('deleting error: '+f+' is not exist.')

   def prepMetGrid(self,fSST,iFcstHour=9,nProc=8,iRound=6):
      """METGRID : iFcstHour (hours), interval (sec)
         [2.0] SUF5_LC05_MKBG_PREP_MGRD (WRF WPS-METGRID PROCESS)
         ------------------------------------------------------------------------
         + INPUT FILES
         ------------------------------------------------------------------------
          1. ${SUF5DAIO}/YYYYMMDD/suf5_lc05_mkbg_bndy_usst.$ANLTIM
          2. /op1/nwp/fcst/MODL/KLBG/N512/DAOU/YYYYMMDD/KLBG:YYYY-MM-DD_HH.$BAGTIM ...
             00,06,12,18 UTC -> BAGTIM = ANLTIM - 6 hour
             01,07,13,19 UTC -> BAGTIM = ANLTIM - 7 hour
             02,08,14,20 UTC -> BAGTIM = ANLTIM - 8 hour
             03,09,15,21 UTC -> BAGTIM = ANLTIM - 3 hour
             04,10,16,22 UTC -> BAGTIM = ANLTIM - 4 hour
             05,11,17,23 UTC -> BAGTIM = ANLTIM - 5 hour
             YYYY-MM-DD_HH : FROM ANLTIM TO ANLTIM+24, 1 hourly( total : 24 files )
          3. /op1/nwp/fcst/ANAL/KL05/N512/DAOU/lapsprd/lapsprep/wps/LAPS:SYY-SMM-SDD_SHH
             SYY-SMM-SDD_SHH : ANLTIM (Only 1 file)
         ------------------------------------------------------------------------
         + ESSENTIAL FILES
         ------------------------------------------------------------------------
          1. $SUF5EXET/suf5_lc05_mkbg_prep_mgrd.e : exe file
          2. $SUF5DABA/geo_em.d01-MKBG.nc         : Static(Terrain etc) file
          3. $SUF5DABA/geo_em.d02-MKBG.nc         : Static(Terrain etc) file
          4. $SUF5DABA/megrid/METGRID.TBL         : Directory(METGRID.TBL)
         ------------------------------------------------------------------------
         + OUTPUT FILES
         ------------------------------------------------------------------------
          1. $SUF5LOGO/met_em_d01.YYYY-MM-DD_HH:00:00
             YYYY-MM-DD_HH:00:00 : FROM ANLTIM TO ANLTIM+6, every 30 minute ( total : 7 files )
             It will be removed by suf5_lc05_mkbg_prep_init_latb.csh
         ------------------------------------------------------------------------
      """
      # set end time of the simulation
      etime = self.stime + datetime.timedelta(hours=iFcstHour)

      if not os.path.isfile(fSST):
         self.log.error('(SST)'+fSST+' is not exist.'); return True

      # set simulation time of background field
      # 00,06,12,18 => t - 6         # 01,07,13,19 => t - 7
      # 02,08,14,20 => t - 8         # 03,09,15,21 => t - 3
      # 04,10,16,22 => t - 4         # 05,11,17,23 => t - 5
      rHours = self.stime.hour%iRound
      tBag = self.stime - datetime.timedelta(hours=rHours\
                                  ,minutes=self.stime.minute\
                                  ,seconds=self.stime.second\
                                  ,microseconds=self.stime.microsecond)
      # use files before 6 hours, if elapsed time is less than 3 hours
      if rHours < iRound/2: tBag -= datetime.timedelta(hours=iRound)

      for f in self.ECMWF: # ex) 03_2_SM1 => SM1 ; the 2nd 10days in the month
         n10days = min((self.stime.day-1) // 10 + 1, 3)
         srcF = str(self.stime.month).zfill(2)+'_'+str(n10days)+'_'+f
         srcF = os.path.join(self.dECMWF,srcF)
         dstF = os.path.join(self.castdir,f)
         if not symLink(srcF,dstF,self.log): return True
         #self.flist.append(dstF)

      # link files for run & check input file
      t = tBag
      while t <= etime: # 0,3,6,9 (if bgIntervalM = 180)
         notLinked = True
         sKLBG = t.strftime("KLBG:%Y-%m-%d_%H")
         for h in range(0,12+1,6): # 0,6,12
            b = tBag - datetime.timedelta(hours=h) # 0,-6,-12
            # ex) 20150501/KLBG:2015-05-01_15.2015050112
            fname = b.strftime("%Y%m%d/")+sKLBG+b.strftime(".%Y%m%d%H")
            srcF = os.path.join(self.KLBGDAOU,fname)
            if os.path.isfile(srcF):
               dstF = os.path.join(self.castdir,sKLBG)
               symLink(srcF,dstF,self.log) #; self.flist.append(dstF)
               self.log.info(" input file : %s, ready "%srcF)
               notLinked = False
               break
            else:
               self.log.warn(srcF+' is not exist. Trying to get another time.')
         if notLinked:
            self.log.error(sKLBG+' is not exist. Check KLBG_LC15(?) results.')
            return True

         t += datetime.timedelta(minutes=self.bgIntervalM)

      # link LAPS analysis file from ~/MODL/KLFS/DAIN/ANLYMD
      sLAPS = self.stime.strftime("LAPS:%Y-%m-%d_%H")
      srcF = os.path.join(self.KLFSDAIN,self.stime.strftime("%Y%m%d"),sLAPS)
      if os.path.isfile(srcF):
         self.log.info(srcF+' is exist. It will replace KLBG.')
         dstF = os.path.join(self.castdir,sLAPS)
         symLink(srcF,dstF,self.log) #; self.flist.append(dstF)
         # Check below if needed. ######################################
         delKLBG = os.path.join(self.castdir\
                  ,self.stime.strftime("KLBG:%Y-%m-%d_%H"))
         if os.path.isfile(delKLBG):
            os.remove(delKLBG) #; self.flist.remove(delKLBG)
      else:
         self.log.warn(srcF+' is not exist.'\
                           +' We have to run SUF5_LC05_MKBG as cold start.')

      # create namelist.wps file
      replaceNML = {\
         'start_date1':self.stime.strftime("%Y-%m-%d_%H:00:00")\
        ,'start_date2':self.stime.strftime("%Y-%m-%d_%H:00:00")\
        ,'end_date1'  :     etime.strftime("%Y-%m-%d_%H:00:00")\
        ,'end_date2'  :     etime.strftime("%Y-%m-%d_%H:00:00")\
        ,'interval_seconds':str(int(self.bgIntervalM*60))\
        ,'constname'  :fSST\
        ,'metgrid_path':self.castdir }
      ifname = os.path.join(self.datadir,'namelist.wps_metgrid')
      ofname = os.path.join(self.castdir,'namelist.wps')
      nmlFile = copyReplace(replaceNML,ifname,ofname,self.log)
      if nmlFile: pass #self.flist.append(nmlFile)
      else: return True

      # run SUF6_LC06_MKBG_PREP_MGRD
      command = "aprun -n %s %s"%(str(nProc),self.prepMetGrid_x)
      quename = self.quename+'@'+self.supercom #normal@haedam
      queSub(command,self.castdir,quename\
            ,'W:BG:MG',str(nProc),'00:10:00',self.log)
      # add deleted file list : met_em.d01.2015-02-28_02:00:00.nc, ..., ...
      for f in glob(os.path.join(self.castdir,'met_em.*.nc')): self.flist.append(f)

      # Rename 'metgrid.log.0000' -> 'metgrid.log' and delete 'metgrid.log.????'
      mgLog = os.path.join(self.castdir,'metgrid.log')
      if os.path.isfile(mgLog+'.0000'): os.rename(mgLog+'.0000',mgLog)
      for f in glob(mgLog+'.????'): self.flist.append(f)
      self.log.info('rename metgrid.log.0000 -> metgrid.log and delete metgrid.log.????')

      self.log.info('[1.0] suf5_lc05_mkbg_prep_mgrd SUCESSFUL FINISHED')
      self.log.info('      See metgrid.log in WiseBG for detail information.')
      return False

   def initLatB( self, iFcstHour=9, nProc=8 ):
      """[2.0] MAKE INIT & BOUNDARY CONDITION
      OUTPUT : SUF5DAIO/suf5_lc05_mkbg_prep_data_init1.YYYYMMDDHH -> wrfinput_d01
      OUTPUT : SUF5DAIO/suf5_lc05_mkbg_prep_data_init2.YYYYMMDDHH -> wrfinput_d02
      OUTPUT : SUF5DAIO/suf5_lc05_mkbg_prep_data_latb.YYYYMMDDHH  -> wrfbdy_d01
      """
      etime = self.stime + datetime.timedelta(hours=iFcstHour)

      # create namelist.input file
      replaceNML = {\
         'run_hours'  :str(iFcstHour)\
        ,'start_year' :self.stime.strftime("%Y, %Y, %Y")\
        ,'start_month':self.stime.strftime("%m, %m, %m")\
        ,'start_day'  :self.stime.strftime("%d, %d, %d")\
        ,'start_hour' :self.stime.strftime("%H, %H, %H")\
        ,'end_year'   :     etime.strftime("%Y, %Y, %Y")\
        ,'end_month'  :     etime.strftime("%m, %m, %m")\
        ,'end_day'    :     etime.strftime("%d, %d, %d")\
        ,'end_hour'   :     etime.strftime("%H, %H, %H")\
        ,'interval_seconds':str(int(self.bgIntervalM*60))\
        ,'history_interval':"%d, %d, %d"%(self.histIntervalM,self.histIntervalM,self.histIntervalM)}
      ifname = os.path.join(self.datadir,'namelist.input_initLatB')
      ofname = os.path.join(self.castdir,'namelist.input')
      nmlFile = copyReplace(replaceNML,ifname,ofname,self.log)
      if nmlFile: pass #self.flist.append(nmlFile)
      else: return True

      # remove 'wrfinput_d??', 'wrfbdy_d??'
      for f in glob(os.path.join(self.castdir,'wrf*')): os.remove(f)

      command = "aprun -n %s %s"%(str(nProc),self.initLatB_x)
      quename = self.quename+'@'+self.supercom #normal@haedam
      queSub(command,self.castdir,quename\
            ,'W:BG:LB',str(nProc),'00:10:00',self.log)

      # Rename 'rsl.{out,error}.0000' -> 'initLatB.{out,error}' and delete 'rsl.{out.error}.*'
      rsl = os.path.join(self.castdir,'rsl')
      if os.path.isfile(rsl+'.out.0000'):
         os.rename(rsl+'.out.0000',os.path.join(self.castdir,'initLatB.out'))
      if os.path.isfile(rsl+'.error.0000'):
         os.rename(rsl+'.error.0000',os.path.join(self.castdir,'initLatB.error'))
      # Remove now because runForecast() produces same file names.
      for f in glob(rsl+'.*'): os.remove(f) # self.flist.append(f)
      self.log.info('rename rsl.{out,error}.0000 -> initLatB.{out,error} and delete remains')

      # Add list to delete : wrfbdy_d01, wrfinput_d01, wrfinput_d02
      for f in glob(os.path.join(self.castdir,'wrf*_d??')): self.flist.append(f)

      self.log.info('[2.0] suf5_lc05_mkbg_init_latb SUCESSFUL FINISHED')
      return False

   def runForecast( self, iFcstHours=(9,6), nProc=256 ):
      """[3.0] RUN Forecast For Background(SUF5 LC05)
      OUTPUT : SUF5DAOU/suf5_lc05_guess_f${ffh}h${ffm}m.$ANLTIM
      OUTPUT : SUF5DAOU/suf5_lc01_guess_f${ffh}h${ffm}m.$ANLTIM
###############################################################################
# [4.0] SUF5_LC05_MKBG_FCST                                                   #
#                                                                             #
#-----------------------------------------------------------------------------#
# + INPUT FILES                                                               #
#-----------------------------------------------------------------------------#
#  1. $SUF5DAIO/ANLYMD/suf5_lc05_mkbg_prep_data_init.$ANLTIM <- wrfinput_d01
#  1. $SUF5DAIO/ANLYMD/suf5_lc01_mkbg_prep_data_init.$ANLTIM <- wrfinput_d02
#  2. $SUF5DAIO/ANLYMD/suf5_lc05_mkbg_prep_data_latb.$ANLTIM <- wrfbdy_d01
#-----------------------------------------------------------------------------#
# + ESSENTIAL FILES
#-----------------------------------------------------------------------------#
#  1. $SUF5EXET/suf5_lc05_mkbg_fcst.e : exe file
#  2. $SUF5DABA/RRTM_DATA
#  3. $SUF5DABA/LANDUSE.TBL
#  4. $SUF5DABA/VEGPARM.TBL
#  5. $SUF5DABA/GENPARM.TBL
#  6. $SUF5DABA/SOILPARM.TBL
#-----------------------------------------------------------------------------#
# + OUTPUT FILES
#-----------------------------------------------------------------------------#
#  1. $SUF5DAOU/ANLYMD/suf5_lc05_guess_f${ffh}h_f${ffm}m.${ANLTIM} (19 files)
#  2. $SUF5DAOU/ANLYMD/suf5_lc01_guess_f${ffh}h_f${ffm}m.${ANLTIM} (13 files)
#-----------------------------------------------------------------------------#
      """
      etime = (self.stime + datetime.timedelta(hours=iFcstHours[0]),\
               self.stime + datetime.timedelta(hours=iFcstHours[1]))

      # Skip to check input data and link
      #   because the needed files are already exist (wrfinput_d??, wrfbdy_d??).

      # create namelist.input file
      #replaceNML = {\
      #   '$run_hours'  :"%d, %d"%iFcstHours\ # Error occured if use diff times.
      replaceNML = {\
         'run_hours'  :"%d"%iFcstHours[0]\
        ,'start_year' :self.stime.strftime("%Y, %Y, %Y")\
        ,'start_month':self.stime.strftime("%m, %m, %m")\
        ,'start_day'  :self.stime.strftime("%d, %d, %d")\
        ,'start_hour' :self.stime.strftime("%H, %H, %H")\
        ,'end_year'   :"%d, %d, %d"%(etime[0].year,etime[1].year,etime[0].year)\
        ,'end_month' :"%d, %d, %d"%(etime[0].month,etime[1].month,etime[0].month)\
        ,'end_day'    :"%d, %d, %d"%(etime[0].day,etime[1].day,etime[0].day)\
        ,'end_hour'   :"%d, %d, %d"%(etime[0].hour,etime[1].hour,etime[0].hour)\
        ,'interval_seconds':str(int(self.histIntervalM*60))\
        ,'history_interval':"%d, %d, %d"%(self.histIntervalM,self.histIntervalM,self.histIntervalM)}
      ifname = os.path.join(self.datadir,'namelist.input_mkbgFcst')
      ofname = os.path.join(self.castdir,'namelist.input')
      nmlFile = copyReplace(replaceNML,ifname,ofname,self.log)
      if nmlFile: pass #self.flist.append(nmlFile)
      else: return True

      # run SUF5_LC05_MKBG_FCST
      command = "aprun -n %s %s"%(str(nProc),self.runForecast_x)
      quename = self.quename+'@'+self.supercom #normal@haedam
      queSub(command,self.castdir,quename\
            ,'W:BG:FC',str(nProc),'01:00:00',self.log)

      # Rename 'rsl.{out,error}.0000' -> 'runForecast.{out,error}' and delete 'rsl.{out.error}.*'
      rsl = os.path.join(self.castdir,'rsl')
      if os.path.isfile(rsl+'.out.0000'):
         os.rename(rsl+'.out.0000',os.path.join(self.castdir,'runForecast.out'))
      if os.path.isfile(rsl+'.error.0000'):
         os.rename(rsl+'.error.0000',os.path.join(self.castdir,'runForecast.error'))
      for f in glob(rsl+'.*'): os.remove(f) # self.flist.append(f)
      self.log.info('rename rsl.{out,error}.0000 -> runForecast.{out,error} and delete remains')

      # rename wrfout_d??_YYYY-mm-dd_HH:MM:SS
      #     to suf5_lc0[5,1]_guess_fHHh_fMMm  #.YYYYmmddHH
      for srcF in glob(os.path.join(self.castdir,'wrfout*')):
         bname = os.path.basename(srcF) # wrfout_d??_YYYY-mm-dd_HH:MM:SS
         t = datetime.datetime.strptime(bname,bname[:11]+"%Y-%m-%d_%H:%M:%S")
         sec = (t - self.stime).seconds # total elapsed time (sec)
         hour, sec = divmod(sec, 3600)  # elapsed hour
         minute, sec = divmod(sec, 60)  # elapsed minute and sec

         if bname[8:10] == '02': res = '01' # d02 -> lc01
         else:                   res = '05' # d01 -> lc05
         #dstF = "suf5_lc%s_guess_f%02dh_f%02dm."%(res,hour,minute)\
         #      +self.stime.strftime("%Y%m%d%H")
         dstF = "suf5_lc%s_guess_f%02dh_f%02dm"%(res,hour,minute) # removed time extension
         dstF = os.path.join(self.wrfprd,dstF)
         if os.path.isfile(dstF): os.remove(dstF)
         os.rename(srcF,dstF);
         # Add file list to delete except suf5_lc01_guess_f??h_f??m
         if not 'suf5_lc01' in dstF: self.flist.append(dstF)

         self.log.info("rename: %s to %s"\
           %(os.path.basename(srcF),os.path.basename(dstF)))

      nmlFile = os.path.join(self.castdir,'namelist.output')
      if os.path.isfile(nmlFile): self.flist.append(nmlFile) # to delete later

      self.log.info('[3.0] wr_mkbg_fcst     SUCESSFUL FINISHED')
      return False

   def makeInit( self, iFcstHour=9 ):
      """[4.0]  Make init for WPS
      OUTPUT : SUF5DAOU/YYYYMMDD/KL05:YYYY-MM-DD_HH:MI.YYYYMMDDHH
# [5.0] SUF5_LC05_MAKE_INIT                                                   #
#                                                                             #
#-----------------------------------------------------------------------------#
# + INPUT FILES                                                               #
#-----------------------------------------------------------------------------#
#  1. $SUF5DAOU/ANLYMD/suf5_lc05_guess_f${ffh}h_f${ffm}m #.${ANLTIM}
#
#-----------------------------------------------------------------------------#
# + ESSENTIAL FILES
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
# + OUTPUT FILES
#-----------------------------------------------------------------------------#
      """
      etime = self.stime + datetime.timedelta(hours=iFcstHour)

      ## Link static directory for lfmpost : '../static' in lmfutil.f90
      #staticDir = os.path.join(self.castdir,'static')
      #if not os.path.exists(staticDir):
      #   symLink(os.path.join(self.datadir,'static'),staticDir,self.log)

      # run LFMPOST 5km as bdy for 1km
      # OUT: wrfprd/??????.pcp & wrfprd/d01/grib/YYJJJDD??????.grib
      env = {'NETCDF':self.netcdf\
            ,'PATH'  :os.path.join(self.netcdf,'bin')\
            ,'LD_LIBRARY_PATH':os.path.join(self.netcdf,'lib')\
            ,'LAPS_DATA_ROOT':self.workdir} # the root directory of 'wrfprd'
            #,'LAPS_DATA':os.path.join(self.workdir,'lapsprd/lapsprep/wps')}
      num = 0; pattern = 'suf5_lc05_guess_f??h_f??m' #.??????????' # removed time extension
      proc = [] # For run in parallel.
      for wrffile in sorted(glob(os.path.join(self.wrfprd,pattern))):
         # Usage: lfmpost.exe 'model type' 'filename' 'grid no' 'laps i4time'
         #                    'fcst time (sec)' 'laps_data_root'
         cmd = self.lfmpost_x+' wrf '+wrffile+' 1 '\
              +self.stime.strftime("%Y%m%d%H ")+str(num)+' '+self.workdir
         p = Process(target=runProcess,args=(cmd,self.wrfprd,wrffile,env,None,600))
         #p = Process(target=runProcess,args=(cmd,self.wrfprd,self.log,{},None,600))
         p.start(); proc.append(p)
         self.log.info('running: '+cmd)
         if num != 0: # except first file for check later.
            self.flist.append(wrffile+'.out')
            self.flist.append(wrffile+'.err')

         num += 1
      for p in proc: p.join() # Wait for the worker processes to exit.

      # 0.pcp, 3600.pcp, ..., 32400.pcp
      for f in glob(os.path.join(self.wrfprd,'*.pcp')): self.flist.append(f)

      #  UNGRIB 1km LFMPOST output
      for f in glob(os.path.join(self.gribdir,'GRIBFILE.???')): os.remove(f)
      for f in glob(os.path.join(self.gribdir,'KL05:????-??-??_??*')): os.remove(f)

      # make julian time
      tt = self.stime.timetuple()
      yyjjjhh = "%02d%03d%02d"%(tt.tm_year%100,tt.tm_yday,tt.tm_hour)
      # ex) 2015.05.19 13:00 => 1513913

      # OUT: wrfprd/d01/grib/YYJJJDD000000.grib -> GRIBFILE.AAA
      gribFiles = sorted(glob(os.path.join(self.gribdir,yyjjjhh+'0000??.grib')))
      for srcF,ext in zip(gribFiles,product(ascii_uppercase,repeat=3)):
         AAA = ''.join(ext)
         if AAA == 'ZZZ':
            self.log.error('too many files ( > 265 ).')
            return True
         dstF = os.path.join(self.gribdir,'GRIBFILE.'+AAA)
         self.flist.append(symLink(srcF,dstF,self.log))

      srcF = os.path.join(self.datadir,'Vtable.KL05')
      dstF = os.path.join(self.gribdir,'Vtable')
      self.flist.append(symLink(srcF,dstF,self.log,False)) # link if not exists.

      # create namelist.wps file
      replaceNML = {'start_date':self.stime.strftime("%Y-%m-%d_%H:%M:00")\
                   ,'end_date'  :     etime.strftime("%Y-%m-%d_%H:%M:00")\
                   ,'interval_seconds':str(int(self.histIntervalM*60))}
      ifname = os.path.join(self.datadir,'namelist.wps_ungrib')
      ofname = os.path.join(self.gribdir,'namelist.wps')
      nmlFile = copyReplace(replaceNML,ifname,ofname,self.log)
      if nmlFile: pass #self.flist.append(nmlFile)
      else: return True

      # OUT: wrfprd/d01/grib/KL05:????-??-??_??
      self.log.info('SUF5 LC05 Now suf5_lc05_prep_ugrb')
      runProcess(self.prepUngrib_x,self.gribdir,self.log,timeoutS=600)

      # To delete of wrfprd/d01/grib/YYJJJDD000000.grib
      for f in glob(os.path.join(self.wrfprd,'d01/grib/*.grib')): self.flist.append(f)

      # See prepMetGrid in WiseFC() for deleting wrfprd/d01/grib/KL05:YYYY-mm-dd_HH:MM

      self.log.info('[4.0] suf5_lc05_make_init     SUCESSFUL FINISHED')
      return False

   def run( self, fSST ):
      if self.prepMetGrid(fSST,nProc=8): return True
      if self.initLatB(nProc=32):        return True
      if self.runForecast(nProc=512):    return True
      if self.makeInit():                return True

      return False

