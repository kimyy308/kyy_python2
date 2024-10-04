#!/usr/bin/env python3
# -*- coding: cp949 -*-
"""
SYNOPSIS

###############################################################################
# SUF1_LC01 run(30 minute interval)
###############################################################################
# [1.0] suf1_lc01_prep_mgrd.ksh           : METGRID
# [2.0] suf1_lc01_init_latb.ksh           : MAKE INIT & BOUNDARY CONDITION
# [3.0] suf1_lc01_fcst.ksh                : RUN MAIN FORECAST
# [4.0] suf1_lc01_post_pout.ksh           : RUN P-LEVEL INTERPOLATION
# [5.0] suf1_lc01_post_pack_grib.ksh      : MAKE GRIB-PACK
# [6.0] suf1_lc01_DRAW                    : DRAW FIGUES
###############################################################################

EXAMPLES
       from modWiseFC import function
"""
# Created: May. 2015

import os, datetime, logging
from glob import glob

# import WISE modules
from modWiseUT import runProcess,queSub,symLink,copyReplace


class WiseFC(object): # forecast class
   def __init__( self, config, timer, logger ):
      self.supercom = config.get('OPERATION','supercom')
      self.prepost  = config.get('OPERATION','prepost')
      self.quename  = config.get('OPERATION','quename')
      self.stime    = timer.UTC;     self.tz = 'UTC'
      self.bgIntervalM   = timer.bgIntervalM # the B.C. data interval(min) from KL05
      self.histIntervalM = timer.histIntervalM # output data interval(min)
      self.outIntervalM  = timer.outIntervalM  # last output data interval(min)
      self.log      = logger.add(config.logdir,'WiseFC',config.logLevel)
      self.datadir  = os.path.join(config.datadir,'WiseR1k')
      self.workdir  = config.castdir
      self.castdir  = os.path.join(self.workdir,'WiseFC')
      self.flist    = []

      if not os.path.isdir(self.castdir): os.makedirs(self.castdir)

      self.prepMetGrid_x = os.path.join(config.bindir,'wr1k_prep_mgrd.x')
      self.avg_tsfc_x    = os.path.join(config.bindir,'avg_tsfc.x')
      self.initLatB_x    = os.path.join(config.bindir,'wr1k_init_latb.x')
      self.runForecast_x = os.path.join(config.bindir,'wr1k_run_fcst.x')

      self.dECMWF=os.path.join(config.datadir,'ECMWF/read_nc/OUT/5yr_tendays')
      self.ECMWF    = ( 'SM1','SM2','SM3','SM4'\
                       ,'ST1','ST2','ST3','ST4')

      self.WiseR1k  = { 'geo_em.d01.nc':'geo_em.d01.nc'\
                       ,'METGRID.TBL'  :'METGRID.TBL.ARW'\
                       ,'RRTM_DATA'    :'RRTM_DATA'\
                       ,'GENPARM.TBL'  :'GENPARM.TBL'\
                       ,'LANDUSE.TBL'  :'LANDUSE.TBL'\
                       ,'SOILPARM.TBL' :'SOILPARM.TBL'\
                       ,'VEGPARM.TBL'  :'VEGPARM.TBL'} # static files
      for d,s in self.WiseR1k.items():
         srcF = os.path.join(self.datadir,s)
         dstF = os.path.join(self.castdir,d)
         symLink(srcF,dstF,self.log) #; self.flist.append(dstF)

      self.log.info('SUF1 LC01 ALL JOB START')
      self.log.info('[0.0] SUF1 LC01 ALL-JOBS SUCESSFUL START')

   def __del__( self ):
      if self.log.getEffectiveLevel() != logging.DEBUG:
         for f in self.flist:
            try: os.remove(f) #; self.log.debug('delete: %s'%f)
            except: self.log.warn('deleting error: '+f+' is not exist.')

   def prepMetGrid( self, fSST, iFcstHour=6, nProc=8 ):
      """[1.0] METGRID
      OUTPUT :  SUF1LOGO/met_em.d01.YYYY-MM-DD_HH:00:00.nc
###############################################################################
# [2.0] SUF1_LC01_PREP_MGRD (WRF WPS-METGRID PROCESS)                         #
#                                                                             #
#  $ANLTIM   : YYYYMMDDHH     10 digit   ex) 2011080308
#  $ANLTIMMI : YYYYMMDDHHMI   12 digit   ex) 201108030830
#-----------------------------------------------------------------------------#
# + INPUT FILES                                                               #
#-----------------------------------------------------------------------------#
#  1. ${SUF5DAIO}/YYYYMMDD/suf5_lc05_mkbg_bndy_usst.$BAGTIM
#  2. ${SUF5DAOU}/YYYYMMDD/KL05:YYYY-MM-DD_HH:MI.$BAGTIM ...
#           ex) 201108030800 -> BAGTIM = 201108030700
#           ex) 201108030830 -> BAGTIM = 201108030700
#  3. ${SU01DAOU}/YYYYMMDD/LAPS:SYYYY-SMM-SDD_SHH:SMI
#     SYYYY-SMM-SDD_SHH:SMI : ANLTIM (Only 1 file)
#     if LAPS:SYYYY-SMM-SDD_SHH:SMI is not exist, just cold start run
#-----------------------------------------------------------------------------#
# + ESSENTIAL FILES
#-----------------------------------------------------------------------------#
#  1. $SUF1EXET/suf1_lc01_prep_mgrd.e : exe file
#  2. $SUF1DABA/geo_em.d01.nc         : Static(Terrain etc) file
#  3. $SUF1DABA/METGRID.TBL.ARW       : Directory(METGRID.TBL)
#-----------------------------------------------------------------------------#
# + OUTPUT FILES
#-----------------------------------------------------------------------------#
#  1. $SUF1LOGO/met_em_d01.YYYY-MM-DD_HH:00:00
#     YYYY-MM-DD_HH:00:00 : FROM ANLTIM TO ANLTIM+6, every 1 hour( total : 7 files )
#     It will be removed by ysf1_lc01_prep_init_latb.csh
#-----------------------------------------------------------------------------#
      """
      # set end time of the simulation
      etime = self.stime + datetime.timedelta(hours=iFcstHour)

      if not os.path.isfile(fSST):
         self.log.error('(SST)'+fSST+' is not exist.'); return True

      # set simulation time of background field
      tBag = self.stime
      #if tBag.minute == 0: tBag -= datetime.timedelta(hours=1) # Check if need.

      for f in self.ECMWF: # ex) 03_2_SM1 => SM1 ; the 2nd 10days in the month
         n10days = min((self.stime.day-1) // 10 + 1, 3)
         srcF = str(self.stime.month).zfill(2)+'_'+str(n10days)+'_'+f
         srcF = os.path.join(self.dECMWF,srcF)
         dstF = os.path.join(self.castdir,f)
         if not symLink(srcF,dstF,self.log): return True
         #self.flist.append(dstF)

      # Link background files
      #t = tBag
      #while t <= etime: # 0.0,0.5,1.0,1.5,...,6.0 (every 30 minuts)
      #   sKL05 = t.strftime("KL05:%Y-%m-%d_%H:%M")
      #   #notLinked = True
      #   #for h in range(0,3,1): # 0,1,2
      #   #   b = tBag - datetime.timedelta(hours=h) # 0,-1,-2
      #   #   # ex) 20150501/KL05:2015-05-01_15:00.2015050114
      #   #   fname = sKL05+b.strftime(".%Y%m%d%H")
      #   #   srcF = os.path.join(os.path.join(self.workdir,'wrfprd/d01/grib',fname))
      #   #   if os.path.isfile(srcF):
      #   #      dstF = os.path.join(self.castdir,sKL05)
      #   #      symLink(srcF,dstF,self.log) #; self.flist.append(dstF)
      #   #      self.log.info(" input file : %s, ready "%srcF)
      #   #      notLinked = False
      #   #      break
      #   #   else:
      #   #      self.log.warn(srcF+' is not exist. Trying to get another time.')
      #   #if notLinked:
      #   #   self.log.error(sKL05+' is not exist. Check wrfprd/d01/grib/KL05:*.')
      #   #   return True
      #   srcF = os.path.join(self.workdir,'wrfprd/d01/grib',sKL05)
      #   if os.path.isfile(srcF):
      #      dstF = os.path.join(self.castdir,sKL05)
      #      symLink(srcF,dstF,self.log) #; self.flist.append(dstF)
      #      self.log.info(" input file : %s, ready "%srcF)
      #   else:
      #      self.log.error(sKL05+' is not exist. Check it in wrfprd/d01/grib/')
      #      return True
      #
      #   t += datetime.timedelta(minutes=self.bgIntervalM)

      # Link background files
      KL05s = glob(os.path.join(self.workdir,'wrfprd/d01/grib/KL05:*'))
      if KL05s:
         for srcF in KL05s:
            dstF = os.path.join(self.castdir,os.path.basename(srcF))
            symLink(srcF,dstF,self.log) #; self.flist.append(dstF)
            # link KL05:YYYY-MM-DD_HH -> KL05:YYYY-MM-DD_HH:00 for 'avg_tsfc.x'
            if len(os.path.basename(srcF)) <= 18:
               symLink(srcF,dstF+':00',self.log) #; self.flist.append(dstF)

            self.flist.append(srcF); self.flist.append(dstF)
      else:
         self.log.error('wrfprd/d01/grib/KL05:* are not exist.')
         return True

      # Link LAPS analysis file from ~/MODL/SUF1/DAIN/ANLYMD
      sLAPS = self.stime.strftime("LAPS:%Y-%m-%d_%H:%M")
      srcF = os.path.join(self.workdir,'WiseDA',sLAPS)
      if os.path.isfile(srcF):
         self.log.info(srcF+' is exist. It will replace KL05.')
         symLink(srcF,os.path.join(self.castdir,sLAPS),self.log)

         delKL05 = os.path.join(self.castdir\
                  ,self.stime.strftime("KL05:%Y-%m-%d_%H:%M"))
         if os.path.isfile(delKL05):
            os.remove(delKL05) #; self.flist.remove(delKL05)
      else:
         self.log.warn(srcF+' is not exist.'\
                           +' We have to run SUF1_LC01 as cold start.')

      # create namelist.wps file
      replaceNML = {'start_date':self.stime.strftime("%Y-%m-%d_%H:%M:00")\
                   ,'end_date'  :     etime.strftime("%Y-%m-%d_%H:%M:00")\
                   ,'interval_seconds':str(int(self.histIntervalM*60))\
                   ,'constname' :fSST\
                   ,'metgrid_path':self.castdir }
      ifname = os.path.join(self.datadir,'namelist.wps_metgrid')
      ofname = os.path.join(self.castdir,'namelist.wps')
      nmlFile = copyReplace(replaceNML,ifname,ofname,self.log)
      if nmlFile: self.flist.append(nmlFile)
      else: return True

      # run SUF1_LC01_MKBG_PREP_MGRD
      runProcess(self.avg_tsfc_x,self.castdir,self.log,timeoutS=600) # avg_tsfc
      command = "aprun -n %s %s"%(str(nProc),self.prepMetGrid_x)
      quename = self.quename+'@'+self.supercom #normal@haedam
      queSub(command,self.castdir,quename\
            ,'W:FC:MG',str(nProc),'00:10:00',self.log)
      self.flist.append(os.path.join(self.castdir,'TAVGSFC')) # Check
      for f in glob(os.path.join(self.castdir,'met_em.*.nc')): self.flist.append(f)

      # Rename 'metgrid.log.0000' -> 'metgrid.log' and delete 'metgrid.log.????'
      mgLog = os.path.join(self.castdir,'metgrid.log')
      if os.path.isfile(mgLog+'.0000'): os.rename(mgLog+'.0000',mgLog)
      for f in glob(mgLog+'.????'): self.flist.append(f)
      self.log.info('rename metgrid.log.0000 -> metgrid.log and delete metgrid.log.????')

      self.log.info('[1.0] suf1_lc01_prep_mgrd SUCESSFUL FINISHED')
      self.log.info('      See metgrid.log in WiseFC for detail information.')
      return False

   def initLatB( self, iFcstHour=6, nProc=32 ):
      """[2.0] MAKE INIT & BOUNDARY CONDITION
      OUTPUT : SUF1DAIO/suf1_lc01_prep_data_init.YYYYMMDDHH
      OUTPUT : SUF1DAIO/suf1_lc01_prep_data_latb.YYYYMMDDHH
###############################################################################
# [3.0] SUF1_LC01_INIT_LATB                                                   #
#                                                                             #
#-----------------------------------------------------------------------------#
# + INPUT FILES                                                               #
#-----------------------------------------------------------------------------#
#  1. $SUF1LOGO/met_em.d01.YYYY-MM-DD_HH:MI:00.nc                             #
#    YYYY-MM-DD_HH:MI:00 : FROM ANLTIM TO ANLTIM+6, 1 hourly( total : 7 files )
#-----------------------------------------------------------------------------#
# + ESSENTIAL FILES                                                           #
#-----------------------------------------------------------------------------#
#  1. $SUF1EXET/ysf1_lc01_init_latb.e : exe file
#  2. $SUF1DABA/gribmap.txt
#  3. $SUF1DABA/RRTM_DATA
#  4. $SUF1DABA/RRTM_DATA_DBL
#  5. $SUF1DABA/ETAMPNEW_DATA
#  6. $SUF1DABA/ETAMPNEW_DATA_DBL
#  7. $SUF1DABA/LANDUSE.TBL
#  8. $SUF1DABA/GENPARM.TBL
#  9. $SUF1DABA/VEGPARM.TBL
# 10. $SUF1DABA/SOILPARM.TBL
#-----------------------------------------------------------------------------#
# + OUTPUT FILES
#-----------------------------------------------------------------------------#
#  1. $SUF1DAIO/YYYYMMDD/suf1_lc01_prep_data_init.$ANLTIM
#  2. $SUF1DAIO/YYYYMMDD/suf1_lc01_prep_data_latb.$ANLTIM
#-----------------------------------------------------------------------------#
###############################################################################
      """
      etime = self.stime + datetime.timedelta(hours=iFcstHour)
      #  LINK FILES FOR RUN  & CHECK INPUT FILE
      #  ln -sf  $SUF1DABA/gribmap.txt                      $SUF1LOGO
      #  ln -sf  $SUF1DABA/RRTM_DATA                        $SUF1LOGO
      #  ln -sf  $SUF1DABA/RRTM_DATA_DBL                    $SUF1LOGO
      #  ln -sf  $SUF1DABA/ETAMPNEW_DATA                    $SUF1LOGO
      #  ln -sf  $SUF1DABA/ETAMPNEW_DATA_DBL                $SUF1LOGO
      #  ln -sf  $SUF1DABA/LANDUSE.TBL                      $SUF1LOGO
      #  ln -sf  $SUF1DABA/GENPARM.TBL                      $SUF1LOGO
      #  ln -sf  $SUF1DABA/VEGPARM.TBL                      $SUF1LOGO
      #  ln -sf  $SUF1DABA/SOILPARM.TBL                     $SUF1LOGO

      # create namelist.input file
      replaceNML = {\
         'run_hours'   :str(iFcstHour)\
        ,'start_year'  :self.stime.strftime("%Y, %Y, %Y")\
        ,'start_month' :self.stime.strftime("%m, %m, %m")\
        ,'start_day'   :self.stime.strftime("%d, %d, %d")\
        ,'start_hour'  :self.stime.strftime("%H, %H, %H")\
        ,'start_minute':self.stime.strftime("%M, %M, %M")\
        ,'end_year'    :     etime.strftime("%Y, %Y, %Y")\
        ,'end_month'   :     etime.strftime("%m, %m, %m")\
        ,'end_day'     :     etime.strftime("%d, %d, %d")\
        ,'end_hour'    :     etime.strftime("%H, %H, %H")\
        ,'end_minute'  :     etime.strftime("%M, %M, %M")\
        ,'interval_seconds':str(int(self.histIntervalM*60))\
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
            ,'W:FC:LB',str(nProc),'01:00:00',self.log)

      # Rename 'rsl.{out,error}.0000' -> 'initLatB.{out,error}' and delete 'rsl.{out.error}.*'
      rsl = os.path.join(self.castdir,'rsl')
      if os.path.isfile(rsl+'.out.0000'):
         os.rename(rsl+'.out.0000',os.path.join(self.castdir,'initLatB.out'))
      if os.path.isfile(rsl+'.error.0000'):
         os.rename(rsl+'.error.0000',os.path.join(self.castdir,'initLatB.error'))
      # Remove now because runForecast() produces same file names.
      for f in glob(rsl+'.*'): os.remove(f) # self.flist.append(f)
      self.log.info('rename rsl.{out,error}.0000 -> initLatB.{out,error} and delete remains')

      # Skip to rename followings. (wrfinput_d??, wrfbdy_d??).
      #  because runForecast() uses the names directly.
      #ctime = self.stime.strftime("%Y%m%d%H")
      #srcF = os.path.join(self.castdir,'wrfinput_d01')
      #dstF = os.path.join(self.castdir,'wr1k_mkbg_prep_data_init.',ctime)
      #os.rename(srcF,dstF); self.log.info(dstF+' successfully done')
      #srcF = os.path.join(self.castdir,'wrfbdy_d01')
      #dstF = os.path.join(self.castdir,'wr1k_mkbg_prep_data_latb.',ctime)
      #os.rename(srcF,dstF); self.log.info(dstF+' successfully done')

      # Add list to delete : wrfbdy_d01, wrfinput_d01, wrfinput_d02
      for f in glob(os.path.join(self.castdir,'wrf*_d??')): self.flist.append(f)

      self.log.info('[2.0] suf1_lc01_init_latb SUCESSFUL FINISHED')
      return False

   def runForecast( self, iFcstHour=6, nProc=512 ):
      """[3.0] RUN Forecast For Background(SUF1 LC01)
      OUTPUT : SUF1DAOU/suf1_lc01_fcst.$ANLTIM
###############################################################################
# [4.0] SUF1_LC01_FCST                                                        #
#                                                                             #
#-----------------------------------------------------------------------------#
# + INPUT FILES                                                               #
#-----------------------------------------------------------------------------#
#  1. $SUF1DAIO/ANLYMD/suf1_lc01_prep_data_init.$ANLTIMMI
#  2. $SUF1DAIO/ANLYMD/suf1_lc01_prep_data_latb.$ANLTIMMI
#-----------------------------------------------------------------------------#
# + ESSENTIAL FILES
#-----------------------------------------------------------------------------#
#  1. $SUF1EXET/suf1_lc01_fcst.e : exe file
#  2. $SUF1DABA/gribmap.txt
#  3. $SUF1DABA/RRTM_DATA
#  4. $SUF1DABA/RRTM_DATA_DBL
#  5. $SUF1DABA/ETAMPNEW_DATA
#  6. $SUF1DABA/ETAMPNEW_DATA_DBL
#  7. $SUF1DABA/LANDUSE.TBL
#  8. $SUF1DABA/GENPARM.TBL
#  9. $SUF1DABA/VEGPARM.TBL
# 10. $SUF1DABA/SOILPARM.TBL
#-----------------------------------------------------------------------------#
# + OUTPUT FILES
#-----------------------------------------------------------------------------#
#  1. $SUF1DAOU/ANLYMD/suf1_lc01_fcst.$ANLTIMMI
#-----------------------------------------------------------------------------#
###############################################################################
      """
      etime = self.stime + datetime.timedelta(hours=iFcstHour)

      # Skip to check input data and link
      #   because the needed files are already exist (wrfinput_d??, wrfbdy_d??).

      # create namelist.input file
      replaceNML = {\
         'run_hours'   :str(iFcstHour)\
        ,'start_year'  :self.stime.strftime("%Y, %Y, %Y")\
        ,'start_month' :self.stime.strftime("%m, %m, %m")\
        ,'start_day'   :self.stime.strftime("%d, %d, %d")\
        ,'start_hour'  :self.stime.strftime("%H, %H, %H")\
        ,'start_minute':self.stime.strftime("%M, %M, %M")\
        ,'end_year'    :     etime.strftime("%Y, %Y, %Y")\
        ,'end_month'   :     etime.strftime("%m, %m, %m")\
        ,'end_day'     :     etime.strftime("%d, %d, %d")\
        ,'end_hour'    :     etime.strftime("%H, %H, %H")\
        ,'end_minute'  :     etime.strftime("%M, %M, %M")\
        ,'interval_seconds':str(int(self.histIntervalM*60))\
        ,'history_interval':"%d, %d, %d"%(self.outIntervalM,self.outIntervalM,self.outIntervalM)}
        #,'history_interval':"%d, %d, %d"%(self.histIntervalM,self.histIntervalM,self.histIntervalM)}
      ifname = os.path.join(self.datadir,'namelist.input_runFcst')
      ofname = os.path.join(self.castdir,'namelist.input')
      nmlFile = copyReplace(replaceNML,ifname,ofname,self.log)
      if nmlFile: pass #self.flist.append(nmlFile)
      else: return True

      # run SUF1_LC01_FCST
      command = "aprun -n %s %s"%(str(nProc),self.runForecast_x)
      quename = self.quename+'@'+self.supercom #normal@haedam
      queSub(command,self.castdir,quename\
            ,'W:FC:FC',str(nProc),'01:00:00',self.log)

      # Rename 'rsl.{out,error}.0000' -> 'runForecast.{out,error}' and delete 'rsl.{out.error}.*'
      rsl = os.path.join(self.castdir,'rsl')
      if os.path.isfile(rsl+'.out.0000'):
         os.rename(rsl+'.out.0000',os.path.join(self.castdir,'runForecast.out'))
      if os.path.isfile(rsl+'.error.0000'):
         os.rename(rsl+'.error.0000',os.path.join(self.castdir,'runForecast.error'))
      for f in glob(rsl+'.*'): os.remove(f) # self.flist.append(f)
      self.log.info('rename rsl.{out,error}.0000 -> runForecast.{out,error} and delete remains')

      #wrfprd = os.path.join(self.castdir,'wrfprd')
      #if not os.path.isdir(wrfprd): os.makedirs(wrfprd)
      # rename wrfout_d??_YYYY-mm-dd_HH:MM:SS to suf1_lc01_fcst.YYYYmmddHH
      for srcF in glob(os.path.join(self.castdir,'wrfout*')):
         bname = os.path.basename(srcF) # wrfout_d??_YYYY-mm-dd_HH:MM:SS
         t = datetime.datetime.strptime(bname,bname[:11]+"%Y-%m-%d_%H:%M:%S")

         if bname[8:10] == '02': res = '01' # d02 -> lc01
         else:                   res = '05' # d01 -> lc05
         dstF = "suf1_lc%s_fcst."%res+self.stime.strftime("%Y%m%d%H")
         #dstF = os.path.join(wrfprd,dstF)
         dstF = os.path.join(self.castdir,dstF)
         #if os.path.isfile(dstF): os.remove(dstF)
         #os.rename(srcF,dstF);    #self.flist.append(dstF)
         symLink(srcF,dstF,self.log)

         self.log.info("link %s to %s"\
           %(os.path.basename(srcF),os.path.basename(dstF)))

      nmlFile = os.path.join(self.castdir,'namelist.output')
      if os.path.isfile(nmlFile): self.flist.append(nmlFile) # to delete later

      self.log.info('[3.0] suf5_lc05_fcst     SUCESSFUL FINISHED')
      return False

   def run( self, fSST ):
      if self.prepMetGrid(fSST,nProc=8): return True
      if self.initLatB(nProc=32):        return True
      if self.runForecast(nProc=512):    return True

      return False

