#!/usr/bin/env python3
# -*- coding: cp949 -*-
"""
SYNOPSIS
       The module for WiseDA

EXAMPLES
       from modWiseDA import function
"""
# Created: Mar. 2015

import os, shutil, datetime, logging
import ftplib, gzip, glob, bz2 #, pycurl
from multiprocessing import Process

# import WISE modules
from modWiseUT import WiseLog, WiseFTP, runProcess, symLink, copyReplace

class WiseObs(object): # Observation Class
   def __init__( self, name, config, stime, logger ):
      self.name = name

      self.stime  = stime
      self.dtime  = stime # dtime will be updated by find() later.
      self.log    = logger.add(config.logdir,self.name,config.logLevel)
      self.wFTP   = WiseFTP(config.comis3host,config.comis3id,config.comis3pw\
                   ,self.log)
      self.url    = config.url
      self.flist  = []; self.dlist = []; self.ilist = []

      self.obsdir  = config.get('COMIS3',self.name)
      self.bindir  = os.path.join(config.bindir,self.name)
      self.datadir = os.path.join(config.datadir,self.name)
      self.workdir = config.castdir
      # Note) Do not change castdir because it must be just under $LAPS_DATA_ROOT
      self.castdir = os.path.join(self.workdir,self.name)
      if not os.path.isdir(self.castdir):
         os.makedirs(os.path.join(self.castdir,'ouda'))
         os.makedirs(os.path.join(self.castdir,'temp'))
      self.oudadir = os.path.join(self.castdir,'ouda')
      self.netcdf  = config.netcdf

   def __del__( self ):
      if self.log.getEffectiveLevel() != logging.DEBUG:
         flist = self.flist+self.dlist+self.ilist
         for f in flist:
            try: os.remove(f) #; self.log.debug('delete: %s'%f)
            except: self.log.warn('deleting error: '+f+' is not exist.')

   #def curl( self, source, target ):
   #   with open(target,'wb') as f:
   #      c = pycurl.Curl()
   #      c.setopt(c.URL, source)
   #      c.setopt(c.USERPWD,'%s:%s'%(self.cfg.comis3id,self.cfg.comis3pw))
   #      c.setopt(c.WRITEDATE, f)
   #      c.perform();             msg = c.errstr()
   #      c.close()
   #   return msg

   def ftp( self, flist ):
      self.log.info('Start getting %s'%self.name)
      for l in flist:
         #if l.startswith('/'): target = self.workdir+    l
         #else:                 target = self.workdir+'/'+l
         #dir,file = os.path.split(target)
         #if not os.path.isdir(self.castdir): os.makedirs(self.castdir)

         target = os.path.join(self.castdir,os.path.basename(l))
         self.flist.append( self.wFTP.get(l,target) )

      self.log.info('End getting %s'%self.name)
      return self.flist

   def run( self ):
      flist = self.get(self.dtime)
      if flist:
         dlist = self.decode(flist)
         if dlist:
            ilist = self.ingest(dlist)
         else: return True
      else: return True

      return False

class KmaMTSAT(WiseObs): # 05km MTSAT-1R DATA
   """KLAPS 05KM MTSAT-1R DATA FTP
      OUTFILE: lapsprd/lvd/mtsat2/YYYYJJJDDHH.lvd
   """
   def __init__( self, config, timer, logger ):
      self.name   = "MTSAT"; self.header = "MTSAT_"
      self.timer  = timer;   self.tz = timer.UTC
      self.channel = {"RIR":"11u","RI2":"12u","RSW":"4u","RWV":"wv","RVI":"vis"}
      super(KmaMTSAT,self).__init__(self.name, config, self.tz, logger)

      self.ingest_x = os.path.join(self.bindir,"wr_sat_lvd.x")
      self.files4ingest = ( "lvd.cdl","form.lvd","mtsat2_ctbl_1024.ir1"\
                           ,"mtsat2_ctbl_1024.ir2","mtsat2_ctbl_1024.ir3"\
                           ,"mtsat2_ctbl_1024.ir4","mtsat2_ctbl_1024.vis"\
                           ,"static.nest7grid" )

   def find( self, dtime, channel, retry=0 ):
      #ex) pattern = "/201502/28/MTSAT_R??_20150228HHMM.bin.gz"
      pattern = dtime.strftime('/%Y%m/%d/')+self.header+channel\
               +dtime.strftime('_%Y%m%d%H%M')+'.bin.gz'
      pattern = self.obsdir+pattern
      list = self.wFTP.nlist(pattern) #get file name by current time
      if retry <= 60 and len(list) == 0: # Recursive 60 times backward by 1 min.
         return self.find(dtime-datetime.timedelta(minutes=1)\
                              ,channel,retry+1)

      if self.dtime > dtime: self.dtime = dtime
      return list

   def get( self, dtime ):
      flist = []
      for c in list(self.channel.keys()): flist = flist+self.find(dtime,c)

      return super(KmaMTSAT,self).ftp(flist)

   def decode( self, flist ):
      """ 05KM MTSAT-1R DATA DECODE : uncompress gzip file and rename it.
          INFILE  : ${SU01DAIN}/MTSAT_RIR_YYYYMMDDHH33.bin.gz
                    ${SU01DAIN}/MTSAT_RI2_YYYYMMDDHH33.bin.gz
                    ${SU01DAIN}/MTSAT_RSW_YYYYMMDDHH33.bin.gz
                    ${SU01DAIN}/MTSAT_RWV_YYYYMMDDHH33.bin.gz
                    ${SU01DAIN}/MTSAT_RVI_YYYYMMDDHH33.bin.gz
          TMPFILE : ${SU01DAIO}/MSAT/temp/
          OUTFILE : ${SU01DAIO}/MSAT/ouda/YYJJJHH00_11u
                    ${SU01DAIO}/MSAT/ouda/YYJJJHH00_12u
                    ${SU01DAIO}/MSAT/ouda/YYJJJHH00_4u
                    ${SU01DAIO}/MSAT/ouda/YYJJJHH00_wv
                    ${SU01DAIO}/MSAT/ouda/YYJJJHH00_vis
      """
      for iFile in flist:
         d,f = os.path.split(iFile) # ex)".../MTSAT_R??_20150228HHMM.bin.gz"
         tt=datetime.datetime.strptime(f,f[:10]+'%Y%m%d%H%M.bin.gz').timetuple()
         yyjjjhhmm = str('%d%03d%02d%02d'%(tt.tm_year,tt.tm_yday\
                        ,tt.tm_hour,tt.tm_min)) # ex)20150561233
         # Check) yyjjjhhmm must be UTC ?
         gzFile = gzip.GzipFile(iFile,'rb')
         s = gzFile.read(); gzFile.close() # uncompress and read a gzip file
         oFName = d + '/ouda/'+yyjjjhhmm+'_'+self.channel[f[6:9]] # ex) f[6:9]='RIR'
         with open(oFName,'wb') as oFile: oFile.write(s) # ex) 20150561233_11u
         self.dlist.append(oFName)

      return self.dlist

   def ingest( self, dlist ):
      # link MTSAT data files if exist.
      for f in self.files4ingest:
         srcF = os.path.join(self.datadir,f)
         dstF = os.path.join(self.castdir,f)
         self.ilist.append(symLink(srcF,dstF,self.log))

      #tt=self.dtime.timetuple()
      #if self.dtime.minute >= 30: mm = 30
      #else:                  mm =  0
      #yyjjjhhmm = str('%02d%03d%02d%02d'%(tt.tm_year%100,tt.tm_yday\
      #   ,tt.tm_hour,tt.tm_min)) # ex)150561230 # Check) yyjjjhh30 must be UTC ?
      yyjjjhhmm = os.path.basename(dlist[0])[:11]
      #self.log.info('DATE: '+self.dtime.strftime("%y%m%d%H%M")+' '+yyjjjhhmm)

      with open(os.path.join(self.datadir,'namelist.input'),'r') as f:
         namelist = f.read()
      context = {"yyjjjhhmm":yyjjjhhmm,"ANLTIM":self.dtime.strftime('%Y%m%d%H%M')}
      nmlFile = os.path.join(self.castdir,'namelist.input')
      with open(nmlFile,'w') as f: f.write(namelist.format(**context))
      self.ilist.append(nmlFile)

      # For ldf field in static.nest7grid file (satvis_process.f)
      lvdName = yyjjjhhmm+'.lvd'
      lvdFile = os.path.join(self.castdir,lvdName)
      try: shutil.copyfile(os.path.join(self.datadir,'form.lvd'),lvdFile)
      except: raise RuntimeError

      for srcF in dlist: # link files if exist.
         d,f = os.path.split(srcF) # split directory and file name
         dstF =os.path.join(self.castdir,f)
         self.ilist.append(symLink(srcF,dstF,self.log))

      env = {"static_nest7grid":os.path.join(self.castdir,'static.nest7grid')\
           ,"LD_LIBRARY_PATH":"${LD_LIBRARY_PATH}:/opt/kma/netcdf/3.6.3.0/netcdf-pgi/lib"}
      #runProcess([self.ingest_x,lvdFile],self.castdir,self.log,env,timeoutS=600)
      runProcess(self.ingest_x+' '+lvdFile,self.castdir,self.log,env,timeoutS=600)

      dstF = os.path.join(self.workdir,'lapsprd/lvd/mtsat2',lvdName)
      os.rename(lvdFile,dstF)

      return self.ilist

class KmaAWS(WiseObs): # 05KM AWS DATA FTP
   """KLAPS 05KM AWS DATA
      OUTFILE: lapsprd/aws/YYYYJJJDDHH.aws
               lapsprd/suv/YYYYJJJDDHH.suv
   """
   def __init__( self, config, timer, logger ):
      self.name   = "AWS"; self.header = "AWSM_"
      self.timer  = timer; self.tz = timer.KST
      super(KmaAWS,self).__init__(self.name, config, self.tz, logger)

      #self.dtime += datetime.timedelta(hours=9) # Don't needed. It is already KST.

      self.decode_x = os.path.join(self.bindir,'wr_aws_decode.x')
      self.ingest_x = os.path.join(self.bindir,'wr_aws_ingest.x')

      # Link AWS site information file
      self.siFile   = os.path.join(self.datadir,'stn_aws.dat')
      siFile = os.path.join(self.oudadir,os.path.basename(self.siFile))
      self.dlist.append(symLink(self.siFile,siFile,self.log))

   def get( self, dtime, deltaHour=0 ):
      #ex) pattern = "/201502/28/AWSM_20150228"
      flist = []
      for h in range(deltaHour+1):
         t = dtime-datetime.timedelta(hours=h)
         pattern = t.strftime('/%Y%m/%d/')+self.header+t.strftime('%Y%m%d%H')
         pattern = self.obsdir+pattern
         flist = flist + self.wFTP.nlist(pattern) #get file name by current time

      return super(KmaAWS,self).ftp(flist)

   def decode( self, flist ):
      """05KM AWS DATA DECODE :
         INFILE  : ${SU01DAIN}/AWSM_YYYYMMDDHH (LST)
         OUTFILE : ${SU01DAIO}/AWSD/ouda/MIN10_AWSYYYYMMDDHH (LST)
      """
      #ctime = self.dtime.strftime('%Y%m%d%H%M')
      for l in flist:
         d,f = os.path.split(l) # split directory and file name
         ctime = f[len(self.header):]+self.dtime.strftime('%M')
         #runProcess([self.decode_x,l,ctime],self.oudadir,self.log,timeoutS=600)
         cmd = self.decode_x+' '+l+' '+ctime
         runProcess(cmd,self.oudadir,self.log,timeoutS=600)
         self.dlist.append(os.path.join(self.oudadir,'MIN10_AWS'+ctime))

      return self.dlist

   def ingest( self, dlist ):
      """KLAPS 05KM AWS DATA INGEST
         INFILE  : ${SU01DAIO}/AWSD/ouda/MIN10_AWSYYYYMMDDHH (LST)
         OUTFILE : ${SU01DAOU}/lapsprd/aws/YYJJJHH00.aws
                   ${SU01DAOU}/lapsprd/suv/YYJJJHH00.suv
      """
      #with open(os.path.join(self.workdir,'static/obs_driver.nl.pre'),'r') as f:
      #   namelist = f.read()
      #context = {"workdir":self.castdir}
      #nmlFile = os.path.join(self.workdir,'static/obs_driver.nl')
      #with open(nmlFile,'w') as f: f.write(namelist.format(**context))
      #self.ilist.append(nmlFile)

      #symLink(os.path.join(self.workdir,'time/systime.dat')\
      #       ,os.path.join(self.oudadir,'systime.dat'),self.log)
      env = {'LAPS_DATA_ROOT':self.workdir}
      #      ,'LAPS_DATA':os.path.join(self.workdir,"lapsprd/lapsprep/wps")}
      runProcess(self.ingest_x,self.oudadir,self.log,env,timeoutS=600)

      return self.ilist

class KmaAMEDAS(WiseObs): # 05KM AMEDAS DATA FTP
   """KLAPS 05KM AMEDAS DATA
      OUTFILE: lapsprd/ads/YYYYJJJDDHH.ads
   """
   def __init__( self, config, timer, logger ):
      self.name  = "AMEDAS"; self.header = "RJTD_ISYA_"
      self.timer = timer;    self.tz = timer.UTC
      super(KmaAMEDAS,self).__init__(self.name, config, self.tz, logger)

      self.decode_x = os.path.join(self.bindir,"wr_amedas_decode.x")
      self.ingest_x = os.path.join(self.bindir,"wr_amedas_ingest.x")

      # Check) running AMEDAS DATA only at 00 minute - check later!
      if self.dtime.minute != 0:
         self.log.warning('AMEDAS data runs only at 00 minute originally but run anyway.')

   def get( self, dtime, retry=0 ):
      #ex) pattern = "/201502/28/RJTD_ISYA_20150228HH.bfr"
      pattern = dtime.strftime('/%Y%m/%d/')+self.header\
               +dtime.strftime('%Y%m%d%H')+'.bfr'
      pattern = self.obsdir+pattern
      list = self.wFTP.nlist(pattern) #get file name by current time
      if retry <= 3 and len(list) == 0: # Recursive 3 times backward by 1 hour.
         return self.get(dtime-datetime.timedelta(hours=1),self.header,retry+1)

      return super(KmaAMEDAS,self).ftp(list)

   def decode( self, flist ):
      """05KM AMEDAS DATA DECODE:
         http://172.20.134.58/url/amedas.php?tm=201502280200
      """
      # Check later why KST used.
      cmd = self.decode_x+' '+self.timer.KST.strftime('%Y%m%d%H')+'00' # Check why 00 only.
      oFile = os.path.join(self.oudadir,'AMEDAS_'+self.dtime.strftime("%Y%m%d%H%M"))
      env = {"URL_IP":self.url, "OUTNAME":oFile}
      runProcess(cmd,self.oudadir,self.log,env,timeoutS=600)

      # `cat ${OUTNAME} | wc -l`
      if os.path.isfile(oFile):
         with open(oFile,'r') as f: count = sum(1 for line in f if line.rstrip('\n'))
      else: count = 0
      self.log.info(" Decoded station num : %s"%count)

      if count > 0: self.dlist.append(oFile)
      else: self.log.warning('No decoded data. Ingest process will be skipped.')

      return self.dlist

   def ingest( self, dlist ):
      """KLAPS 05KM AMEDAS DATA INGEST
         INFILE  : ${SU01DAIO}/AMDS/ouda/AMEDAS_YYYYMMDDHH00
         OUTFILE : ${SU01DAOU}/lapsprd/ads/YYJJJHH00.ads
      """

      # Link site infomation file
      dstF = os.path.join(self.oudadir,'stn_amedas.dat')
      if not os.path.isfile(dstF):
         srcF = os.path.join(self.datadir,'stn_amedas.dat')
         symLink(srcF,dstF,self.log)

      #with open(os.path.join(self.workdir,'static/obs_driver.nl.pre'),'r') as f:
      #   namelist = f.read()
      #context = {"workdir":self.castdir}
      #nmlFile = os.path.join(self.workdir,'static/obs_driver.nl')
      #with open(nmlFile,'w') as f: f.write(namelist.format(**context))
      #self.ilist.append(nmlFile)

      if dlist: # if exists 'ouda/AMEDAS_YYYYMMDDHH00
         env = {"LAPS_DATA_ROOT":self.workdir}
         #       "LAPS_DATA":os.path.join(self.workdir,"lapsprd/lapsprep/wps")}
         runProcess(self.ingest_x,self.oudadir,self.log,env,timeoutS=600)
      else: self.log.info('No decoded data. Skip ingest process.')

class KmaLGT(WiseObs): # 05KM LGT DATA FTP
   """KLAPS 05KM LGT DATA
      OUTFILE: lapsprd/lgt/YYYYJJJDDHH.lgt
   """
   def __init__( self, config, timer, logger ):
      self.name  = "LGT"; self.header = "LGT_KMA_"
      self.timer = timer; self.tz = timer.KST
      super(KmaLGT,self).__init__(self.name, config, self.tz, logger)

      #self.dtime += datetime.timedelta(hours=9) # Don't needed. It is already KST.

      self.decode_x = os.path.join(self.bindir,"wr_lgt_decode.x")
      self.ingest_x = os.path.join(self.bindir,"wr_lgt_ingest.x")

   def get( self, dtime, deltaMin=10 ):
      #ex) pattern = "/2015/LGT_KMA_20150228.asc"
      t0 = dtime; t3 = dtime-datetime.timedelta(hours=deltaMin)
      p0 = t0.strftime('%Y/')+self.header+t0.strftime('%Y%m%d')+'.asc'
      p3 = t3.strftime('%Y/')+self.header+t3.strftime('%Y%m%d')+'.asc'

      list = self.wFTP.nlist(os.path.join(self.obsdir,p0))
      if p0 != p3: list = list + self.wFTP.nlist(os.path.join(self.obsdir,p3))

      return super(KmaLGT,self).ftp(list)

   def decode( self, flist ):
      """05KM LGT DATA DECODE:
         http://172.20.134.58/url/lgt_txt1.php?tm=201502281130
         INFILE  : URL
         OUTFILE : ${SU01DAIO}/LGTD/ouda/lgt_YYYYMMDDHH (LST)
      """
      # Seacrh url from -20 min to +5 min. ex) 11:10 ~ 11:35 if it is 11:30
      cmd = self.decode_x+' '\
           +(self.dtime+datetime.timedelta(minutes=5)).strftime("%Y%m%d%H%M -25")
      oFile = os.path.join(self.oudadir,'lgt_'+self.dtime.strftime("%Y%m%d%H%M"))
      env = {"URL_IP":self.url, "OUTNAME":oFile}
      runProcess(cmd,self.oudadir,self.log,env,timeoutS=600)

      # `cat ${OUTNAME} | wc -l`
      if os.path.isfile(oFile):
         with open(oFile,'r') as f: count = sum(1 for line in f if line.rstrip('\n'))
      else: count = 0
      self.log.info(" Decoded data num : %s"%count)

      if count > 0: self.dlist.append(oFile)
      else: self.log.warning('No decoded data. Ingest process will be skipped.')

      return self.dlist

   def ingest( self, dlist ):
      """KLAPS 05KM LIGHTNING DATA INGEST: lapsprd/lgt
         INFILE  : ${SU01DAIO}/LGT/ouda/lgt_YYYYMMDDHH (LST)
         OUTFILE : ${SU01DAOU}/lapsprd/lgt/YYJJJHH00.lgt
                   ${SU01DAOU}/lapsprd/lgt/YYJJJHH00.lgt_org
      """
      #with open(os.path.join(self.workdir,'static/obs_driver.nl.pre'),'r') as f:
      #   namelist = f.read()
      #context = {"workdir":self.castdir}
      #nmlFile = os.path.join(self.workdir,'static/obs_driver.nl')
      #with open(nmlFile,'w') as f: f.write(namelist.format(**context))
      #self.ilist.append(nmlFile)

      if dlist: # if exists 'ouda/lgt_YYYYMMDDHH
         env = {"LAPS_DATA_ROOT":self.workdir}
         #       "LAPS_DATA":os.path.join(self.workdir,"lapsprd/lapsprep/wps")}
         runProcess(self.ingest_x,self.oudadir,self.log,env,timeoutS=600)
      else: self.log.info('No decoded data. Skip ingest process.')

class KmaWPF(WiseObs): # 05KM KWPF DATA FTP
   """KLAPS 05KM KWPF DATA
      OUTFILE: lapsprd/pro/YYYYJJJDDHH.pro
   """
   def __init__( self, config, timer, logger ):
      self.name   = "KWPF"; self.header = "WPU_"
      self.timer  = timer; self.tz = timer.UTC
      self.station = ("47095","47099","47105","47114","47130","47135"\
                     ,"47140","47151","47155","47167","47229","47261")
      # Check) other stations : 470106, 47153, ...
      super(KmaWPF,self).__init__(self.name, config, self.tz, logger)

      self.decode_x = os.path.join(self.bindir,"wr_wpf_decode.x")
      self.ingest_x = os.path.join(self.bindir,"wr_wpf_ingest.x")

      # Check) running KWPF DATA only at 00 minute - check later!
      if self.dtime.minute != 0:
         self.log.warning('KWPF data runs only at 00 minute originally but run anyway.')

   def find( self, dtime, station, retry=0 ):
      #ex) pattern = "/201502/28/WPU_47229_201502280640.PRO"
      pattern = dtime.strftime('/%Y%m/%d/')+self.header+station\
               +dtime.strftime('_%Y%m%d%H%M')+'.PRO'
      pattern = self.obsdir+pattern
      list = self.wFTP.nlist(pattern) #get file name by current time
      if retry <= 60 and len(list) == 0: # Recursive 60 times backward by 1 min.
         return self.find(dtime-datetime.timedelta(minutes=1)\
                              ,station,retry+1)
      return list

   def get( self, dtime ):
      """KLAPS 05KM KWPF DATA FTP
         WPU_47261_YYYYMMDDHHmm.PRO, WPU_47099_YYYYMMDDHHmm.PRO,
         WPU_47105_YYYYMMDDHHmm.PRO, WPU_47140_YYYYMMDDHHmm.PRO,
         WPU_47155_YYYYMMDDHHmm.PRO, WPU_47151_YYYYMMDDHHmm.PRO,
         WPU_47095_YYYYMMDDHHmm.PRO, WPU_47114_YYYYMMDDHHmm.PRO,
         WPU_47130_YYYYMMDDHHmm.PRO, WPU_47135_YYYYMMDDHHmm.PRO,
         WPU_47167_YYYYMMDDHHmm.PRO, WPU_47229_YYYYMMDDHHmm.PRO
      """
      list = []
      for n in self.station: list = list + self.find(dtime,n)
      return super(KmaWPF,self).ftp(list)

   def decode( self, flist ):
      """05KM KWPF DATA DECODE:
         http://172.20.134.58/url/lgt_txt1.php?tm=201502281130
         OUTFILE : ${SU01DAIO}/KWPF/ouda/WPF_KMA_YYYYMMDDHH00
      """
      # round the minute to the closest 10th minute. ex) 02:32:00 -> 02:30:00
      #cmd = self.decode_x+' '\
      #    +(self.dtime - datetime.timedelta(minutes=self.dtime.minute%10\
      #     ,seconds=self.dtime.second,microseconds=self.dtime.microsecond)\
      #     ).strftime('%Y%m%d%H%M -10')

      # Seacrh url from -10 min to 0 min. ex) 11:20 ~ 11:30 if it is 11:30
      cmd = self.decode_x+' '+self.dtime.strftime('%Y%m%d%H%M -10')
      oFile = os.path.join(self.oudadir,'WPF_KMA_'+self.dtime.strftime("%Y%m%d%H%M"))
      env = {"URL_IP":self.url, "OUTNAME":oFile}
      runProcess(cmd,self.oudadir,self.log,env,timeoutS=600)
      self.dlist.append(oFile)

      return self.dlist

   def ingest( self, dlist ):
      """KLAPS 05KM KWPF DATA INGEST: /lapsprd/pro
         INFILE  : ${SU01DAIO}/KWPF/ouda/WPF_KMA_YYYYMMDDHH00
         OUTFILE : ${SU01DAOU}/lapsprd/pro/YYJJJHH00.pro
      """
      #with open(os.path.join(self.datadir,'nest7grid.parms'),'r') as f:
      #   namelist = f.read()
      #context = {'datadir':self.datadir, 'workdir':self.castdir}
      #nmlFile = os.path.join(self.castdir,'static/nest7grid.parms')
      #with open(nmlFile,'w') as f: f.write(namelist.format(**context))
      #self.ilist.append(nmlFile)

      # Check) Error: src/ingest/profiler/read_profiler_kma.f reads only 00 minute.
      if self.dtime.minute != 0: # Error: src/ingest/profiler/read_profiler_kma.f reads only 00 minute.
         srcF = os.path.join(self.oudadir,'WPF_KMA_'+self.dtime.strftime("%Y%m%d%H%M"))
         dstF = srcF[:len(srcF)-2]+'00'
         symLink(srcF,dstF,self.log)

      env = {"LAPS_DATA_ROOT":self.workdir}
      #       "LAPS_DATA":os.path.join(self.workdir,"lapsprd/lapsprep/wps")}
      runProcess(self.ingest_x,self.oudadir,self.log,env,timeoutS=600)

      # Check) Error: src/ingest/profiler/read_profiler_kma.f reads only 00 minute.
      if self.dtime.minute != 0: # Error: src/ingest/profiler/read_profiler_kma.f reads only 00 minute.
         tt = self.dtime.timetuple()
         srcF = str('%02d%03d%02d00.pro'%(tt.tm_year%100,tt.tm_yday\
                   ,tt.tm_hour))           # ex)150561200
         srcF = os.path.join(self.workdir,'lapsprd/pro',srcF)
         dstF = str('%02d%03d%02d%02d.pro'%(tt.tm_year%100,tt.tm_yday\
                   ,tt.tm_hour,tt.tm_min)) # ex)150561230
         dstF = os.path.join(self.workdir,'lapsprd/pro',dstF)
         symLink(srcF,dstF,self.log)

class KmaMTAR(WiseObs): # 05KM KMETAR DATA FTP
   """KLAPS 05KM KMETAR DATA
      OUTFILE: lapsprd/met/YYYYJJJDDHH.met
   """
   def __init__( self, config, timer, logger ):
      self.name   = "MTAR"; self.headers = ("ADKO60_KMA_","SAKO01_KAF_")
      self.timer  = timer;  self.tz = timer.KST
      super(KmaMTAR,self).__init__(self.name, config, self.tz, logger)

      #self.dtime += datetime.timedelta(hours=9) # Don't needed. It is already KST.

      self.decode_x = os.path.join(self.bindir,"wr_mtar_decode.x")
      self.ingest_x = os.path.join(self.bindir,"wr_mtar_ingest.x")

      # Check) running MTAR DATA only at 00 minute - check later!
      if self.dtime.minute != 0:
         self.log.warning('MTAR data runs only at 00 minute originally but run anyway.')

   def find( self, dtime, header, retry=0 ):
      #ex) pattern1 = "/201502/28/ADKO60_KMA_2015022817"
      #ex) pattern2 = "/201502/28/SAKO01_KAF_2015031017"
      pattern = dtime.strftime('/%Y%m/%d/')+header+dtime.strftime('%Y%m%d%H')
      pattern = self.obsdir+pattern
      list = self.wFTP.nlist(pattern) #get file name by current time
      if retry <= 3 and len(list) == 0: # Recursive 3 times backward by 1 hour.
         return self.find(dtime-datetime.timedelta(hours=1)\
                              ,header,retry+1)
      return list

   def get( self, dtime ):
      """KLAPS 05KM KMETAR DATA FTP
         ADKO60_KMA_YYYYMMDDHH (LST)
         SAKO01_KAF_YYYYMMDDHH (LST)
      """
      list = []
      for h in self.headers: list = list + self.find(dtime,h)
      return super(KmaMTAR,self).ftp(list)

   def decode( self, flist ):
      """05KM MTAR DATA DECODE: KMA & KAF
         http://172.20.134.58/url/air_metar_dec.php?tm=201502280200&org=K
         OUTFILE : ${SU01DAIO}/MTAR/ouda/metar.YYMMDDHH
      """
      # Check) MTAR DATA only at 00 minute - check later!
      #cmd = self.decode_x+' '+self.dtime.strftime('%Y%m%d%H')+'00'
      cmd = self.decode_x+' '+self.dtime.strftime('%Y%m%d%H%M')
      # Check) UTC is used in outfile name
      oFile = os.path.join(self.oudadir,'metar.'+self.timer.UTC.strftime("%Y%m%d%H%M"))
      env = {'URL_IP':self.url, 'OUTNAME':oFile}
      # Check) KMA & KAF read seperately but store those to one file?
      runProcess(cmd,self.oudadir,self.log,env,timeoutS=600)
      self.dlist.append(oFile)

      return self.dlist

   def ingest( self, dlist ):
      """KLAPS 05KM KMETAR DATA INGEST
         INFILE  : ${SU01DAIO}/MTAR/ouda/metar.YYMMDDHH
         OUTFILE : ${SU01DAOU}/lapsprd/met/YYJJJHH00.met
      """

      # Link site infomation file
      dstF = os.path.join(self.oudadir,'stn_metar.dat')
      if not os.path.isfile(dstF):
         srcF = os.path.join(self.datadir,'stn_metar.dat')
         symLink(srcF,dstF,self.log)

      #with open(os.path.join(self.workdir,'static/obs_driver.nl.pre'),'r') as f:
      #   namelist = f.read()
      #context = {"workdir":self.castdir}
      #nmlFile = os.path.join(self.workdir,'static/obs_driver.nl')
      #with open(nmlFile,'w') as f: f.write(namelist.format(**context))
      #self.ilist.append(nmlFile)

      # Check) Error: src/ingest/profiler/read_profiler_kma.f reads only 00 minute.
      if self.timer.UTC.minute != 0:
         srcF = os.path.join(self.oudadir,'metar.'+self.timer.UTC.strftime("%Y%m%d%H%M"))
         dstF = srcF[:len(srcF)-2]+'00'
         symLink(srcF,dstF,self.log)

      env = {"LAPS_DATA_ROOT":self.workdir,\
             "LAPS_DATA":os.path.join(self.workdir,"lapsprd/met")}
      runProcess(self.ingest_x,self.oudadir,self.log,env,timeoutS=600)

      # Check) Error: src/ingest/profiler/read_profiler_kma.f reads only 00 minute.
      if self.timer.UTC.minute != 0:
         tt = self.timer.UTC.timetuple()
         dstF = str('%02d%03d%02d00.met'%(tt.tm_year%100,tt.tm_yday\
                   ,tt.tm_hour))           # ex)150561200
         dstF = os.path.join(self.workdir,'lapsprd/met',dstF)
         srcF = str('%02d%03d%02d%02d.met'%(tt.tm_year%100,tt.tm_yday\
                   ,tt.tm_hour,tt.tm_min)) # ex)150561230
         srcF = os.path.join(self.workdir,'lapsprd/met',srcF)
         symLink(srcF,dstF,self.log)

class KmaAMDAR(WiseObs): # 05KM AMDAR DATA FTP
   """KLAPS 05KM AMDAR DATA
      OUTFILE: lapsprd/pin/YYYYJJJDDHH.pin
   """
   def __init__( self, config, timer, logger ):
      self.name   = "AMDAR"; self.header = "IUAC01_RKSL_"
      self.timer  = timer;   self.tz = timer.UTC
      super(KmaAMDAR,self).__init__(self.name, config, self.tz, logger)

      self.decode_x = os.path.join(self.bindir,"wr_amdr_dcod_kal.x")
      self.ingest_x = os.path.join(self.bindir,"wr_amdr_ingt.x")

   #def get( self, dtime, retry=0 ):
   #   """KLAPS 05KM AMDAR DATA FTP
   #      Note) getting files from -15 min to 5 min in ASAPS."""
   #   #ex) pattern = "/201502/28/IUAC01_RKSL_20152280HHMM.bfr"
   #   pattern = dtime.strftime('/%Y%m/%d/')+self.header\
   #            +dtime.strftime('%Y%m%d%H%M')+'.bfr'
   #   pattern = self.obsdir+pattern
   #   list = self.wFTP.nlist(pattern) #get file name by current time
   #   if retry <= 60 and len(list) == 0: # Recursive 60 times backward by 1 min.
   #      return self.get(dtime-datetime.timedelta(minutes=1),retry+1)
   #
   #   return super(KmaAMDAR,self).ftp(list)

   def get( self, dtime ):
      """KLAPS 05KM AMDAR DATA FTP
         Note) getting files from -15 min to 5 min in ASAPS."""
      list = []
      for m in range(-15,6):
         t = dtime + datetime.timedelta(minutes=m)
         #ex) pattern = "/201502/28/IUAC01_RKSL_20152280HHMM.bfr"
         pattern = t.strftime('%Y%m/%d/')+self.header+t.strftime('%Y%m%d%H%M')+'.bfr'
         pattern = os.path.join(self.obsdir,pattern)
         list += self.wFTP.nlist(pattern) # Get file name

      return super(KmaAMDAR,self).ftp(list)

   def decode( self, flist ):
      """05KM AMDAR DATA DECODE : decoding from -15 min to 5 min
         INFILE  : IUAC01_RKSL_yyyymmddhh5*.bfr (-1 Hour)
                   IUAC01_RKSL_YYYYMMDDHH0*.bfr
         OUTFILE : ${SU01DAIO}/AMDR/ouda/kal_amdar_YYYYMMDDHH.dat
      """
      # link Table data file if not exist.
      self.fileTableB = os.path.join(self.datadir,'TABLE_B.DAT')
      dstF = os.path.join(self.oudadir,'TABLE_B.DAT')
      if not os.path.isfile(dstF):
         srcF = os.path.join(self.datadir,'TABLE_B.DAT')
         symLink(srcF,dstF,self.log)

      lists = ' '.join(flist)
      runProcess(self.decode_x+' '+lists,self.oudadir,self.log,timeoutS=600)

      srcF = os.path.join(self.oudadir,'kal_amdar.dat')
      if os.path.isfile(srcF):
         dstF = 'kal_amdar_'+self.dtime.strftime("%Y%m%d%H%M.dat")
         dstF = os.path.join(self.oudadir,dstF)
         os.rename(srcF,dstF); self.log.info('rename : '+srcF+' -> '+dstF)
         self.dlist.append(dstF)

      return self.dlist

   def ingest( self, dlist ):
      """KLAPS 05KM AMDAR DATA INGEST: lapsprd/pin
         INFILE  : ${SU01DAIO}/AMDR/ouda/kal_amdar_YYYYMMDDHH.dat
         OUTFILE : ${SU01DAOU}/lapsprd/pin/YYJJJHH00.pin
      """
      tt = self.dtime.timetuple()
      yyjjjhhmm = str('%02d%03d%02d%02d'%(tt.tm_year%100,tt.tm_yday\
                 ,tt.tm_hour,tt.tm_min)) # ex)150561230
      for srcF in dlist:
         #srcF = self.dtime.strftime("ouda/kal_amdar_%Y%m%d%H%M.dat")
         dstF = os.path.join(self.workdir,"lapsprd/pin/%s.pin"%yyjjjhhmm)
         try: shutil.copyfile(srcF,dstF)
         except: raise RuntimeError
         self.log.info('copy : '+srcF+' -> '+dstF)

         # `grep Lat srcF | wc -l`
         count = 0
         with open(srcF,'r') as f:
            for w in f.read().split(" "):
               if 'Lat' in w: count += 1
         self.log.info("num of data : %s"%count)

class KmaRADAR(WiseObs): # 05KM RADAR DATA FTP
   """KLAPS 05KM RADAR DATA
      OUTFILE: lapsprd/v??/YYYYJJJDDHH.v??
               lapsprd/lm?/YYYYJJJDDHH.lm?
               lapsprd/lso/YYYYJJJDDHH.lso
               lapsprd/tmp/YYYYJJJDDHH.tmp
               lapsprd/vrc/YYYYJJJDDHH.vrc
               lapsprd/vrz/YYYYJJJDDHH.vrz
   """
   def __init__( self, config, timer, logger ):
      self.name   = "RADAR"; self.header = "RDR_"; self.qctype = "_QCD_"
      self.timer  = timer;   self.tz = timer.KST
      #self.station= ("BRI","DNH","GDK",      "GSN","IIA","JNI","KSN","KWK","MYN","PSN","SSP")
      #self.station= ("BRI",      "GDK","GNG","GSN","IIA","JNI","KSN","KWK","MYN","PSN","SSP")
      self.station = ("BRI",      "GDK","GNG","GSN","IIA","JNI","KSN","KWK","MYN","PSN","SSP","BSL")
      super(KmaRADAR,self).__init__(self.name, config, self.tz, logger)

      #self.dtime += datetime.timedelta(hours=9) # Don't needed. It is already KST.

      self.decode_x = os.path.join(self.bindir,"wr_radar_decode.x")
      self.decode_bsl_x = os.path.join(self.bindir,"wr_radar_decode_bsl.x")
      self.ingest_x = os.path.join(self.bindir,"wr_radar_ingest.x")

   def find( self, dtime, station, retry=0 ):
      # ex) pattern = "/201502/28/RDR_{STN}_QCD_20150228HHMM.uf"
      pattern = dtime.strftime('/%Y%m/%d/')+self.header+station+self.qctype\
               +dtime.strftime('%Y%m%d%H%M')+'.uf'
      pattern = self.obsdir+pattern
      list = self.wFTP.nlist(pattern)    # get file name by current time
      if retry <= 60 and len(list) == 0: # Recursive 60 times backward by 1 min.
         return self.find(dtime-datetime.timedelta(minutes=1),station,retry+1)
      return list

   def get( self, dtime ):
      """KLAPS 05KM RADAR DATA FTP
         RDR_BRI_QCD_YYYYMMDDHHmm.uf (LST), RDR_DNH_QCD_YYYYMMDDHHmm.uf (LST),
         RDR_GDK_QCD_YYYYMMDDHHmm.uf (LST), RDR_GSN_QCD_YYYYMMDDHHmm.uf (LST),
         RDR_IIA_QCD_YYYYMMDDHHmm.uf (LST), RDR_JNI_QCD_YYYYMMDDHHmm.uf (LST),
         RDR_KSN_QCD_YYYYMMDDHHmm.uf (LST), RDR_KWK_QCD_YYYYMMDDHHmm.uf (LST),
         RDR_MYN_QCD_YYYYMMDDHHmm.uf (LST), RDR_PSN_QCD_YYYYMMDDHHmm.uf (LST),
         RDR_SSP_QCD_YYYYMMDDHHmm.uf (LST)
      """
      list = []
      for n in self.station: list = list + self.find(dtime,n)
      return super(KmaRADAR,self).ftp(list)

   def decode( self, flist ):
      """05KM RADAR DATA DECODE
         INFILE  : ${SU01DAIN}/RDR_BRI_QCD_YYYYMMDDHHmm.uf (LST)
                   ${SU01DAIN}/RDR_DNH_QCD_YYYYMMDDHHmm.uf (LST)
                   ${SU01DAIN}/RDR_GDK_QCD_YYYYMMDDHHmm.uf (LST)
                   ${SU01DAIN}/RDR_GSN_QCD_YYYYMMDDHHmm.uf (LST)
                   ${SU01DAIN}/RDR_IIA_QCD_YYYYMMDDHHmm.uf (LST)
                   ${SU01DAIN}/RDR_JNI_QCD_YYYYMMDDHHmm.uf (LST)
                   ${SU01DAIN}/RDR_KSN_QCD_YYYYMMDDHHmm.uf (LST)
                   ${SU01DAIN}/RDR_KWK_QCD_YYYYMMDDHHmm.uf (LST)
                   ${SU01DAIN}/RDR_MYN_QCD_YYYYMMDDHHmm.uf (LST)
                   ${SU01DAIN}/RDR_PSN_QCD_YYYYMMDDHHmm.uf (LST)
                   ${SU01DAIN}/RDR_SSP_QCD_YYYYMMDDHHmm.uf (LST)
         TMPFILE : ${SU01DAIO}/temp/BRI/...
         OUTFILE : ${SU01DAIO}/ouda/BRI/YYJJJHHH00.elev01
                   ${SU01DAIO}/ouda/BRI/YYJJJHHH00.elev02
                   ......
                   ${SU01DAIO}/ouda/SSP/YYJJJHHH00.elev02
      """
      for iFile in flist:
         if not os.path.isfile(iFile): continue

         wDir,fName = os.path.split(iFile) # ex)".../RDR_{STN}_QCD_20150228HHMM.uf"

         # ex) fName = "RDR_STN_QCD_20150228HHMM.uf"
         #tt = datetime.datetime.strptime(fName,fName[:12]+'%Y%m%d%H%M.uf').timetuple()
         tt = self.timer.UTC.timetuple() # Check) UTC or KST
         yyjjjhhmm = str('%02d%03d%02d%02d'%(tt.tm_year%100,tt.tm_yday\
                        ,tt.tm_hour,tt.tm_min)) # ex) 15056HHMM
         # Check) yyjjjhhmm must be UTC ?

         # mkdir -p ${SU01DAIO}/${DSET}/{temp,ouda}/${STN}
         stnName = fName[4:7]
         tempStn = os.path.join(wDir,'temp',stnName)
         if not os.path.isdir(tempStn): os.makedirs(tempStn)
         oudaStn = os.path.join(wDir,'ouda',stnName)
         if not os.path.isdir(oudaStn): os.makedirs(oudaStn)

         runProcess(self.decode_x+' '+iFile,tempStn,self.log,timeoutS=600)

         levFiles = glob.glob(tempStn+'/*'+stnName+'.s??')# ex) ".../*GDK.s??"
         # Check) level files are changed UTC but rechanged to KST with julian
         for lf in levFiles: # level files
            # ex) .../GDK/150591130_elev12 - julian day & level number
            outFile = oudaStn+'/'+yyjjjhhmm+"_elev"+os.path.splitext(lf)[1][2:]
            os.rename(lf,outFile)
            self.log.info('rename : '+lf+' -> '+outFile)
            self.dlist.append(outFile)
            if fName[4:7] == "BSL": # Station Name
               runProcess(self.decode_bsl_x+' '+outFile,tempStn,self.log,timeoutS=600)
               # Check) save output file name to dlist
               #self.dlist.append(oFName)

      return self.dlist

   def ingest( self, dlist ):
      """KLAPS 05KM RADAR DATA INGEST
         INFILE  : ${SU01DAIO}/ouda/BRI/YYJJJHHH00.elev01
                   ${SU01DAIO}/ouda/BRI/YYJJJHHH00.elev02
                   ....
                   ${SU01DAIO}/ouda/SSP/YYJJJHHH00.elev02

         OUTFILE : ${SU01DAOU}/lapsprd/v01/YYJJJHH00.v01
                   ${SU01DAOU}/lapsprd/v02/YYJJJHH00.v02
      """

      nmlFile = os.path.join(self.workdir,'static/remap.nl')
      with open(nmlFile+'.pre','r') as f: namelist = f.read()
      context = {"workdir":self.oudadir}
      with open(nmlFile,'w') as f: f.write(namelist.format(**context))
      self.ilist.append(nmlFile)

      env = {'NETCDF':self.netcdf\
            ,'PATH'  :os.path.join(self.netcdf,'bin')\
            ,'LD_LIBRARY_PATH':os.path.join(self.netcdf,'lib')\
            ,'LAPS_DATA_ROOT':self.workdir}
            #,'LAPS_DATA':os.path.join(self.workdir,'lapsprd/lapsprep/wps')}
      runProcess(self.ingest_x,self.oudadir,self.log,env,timeoutS=600)

      # Add ingest output list to delete later.
      for f in glob.glob(os.path.join(self.workdir,'lapsprd/v??/*')): self.ilist.append(f)

class KmaUSST(): # get SST from UKMO
   """[1.0] USST
      get data from /op1/nwp/prep/ADEX/DCOD/DANR/*YYYYMMDD*OSTIA*.nc.bz2
   """
   def __init__( self, config, timer, logger ):
      self.name   = 'USST'
      self.timer  = timer;      self.tz = timer.UTC
      self.stime  = self.tz
      self.dtime  = self.tz # dtime will be updated by find().
      self.log    = logger.add(config.logdir,self.name,config.logLevel)
      self.flist  = []

      self.bindir = os.path.join(config.bindir,'SST',self.name)
      self.obsdir = config.get('DCOD','DCODDANR')
      self.sst_x  = os.path.join(self.bindir,"wr_mkbg_prep_usst.x")
      self.pattern= '*OSTIA*.nc.bz2'
      self.maxRetry= 7 # max interation of glob (days) to find recent SST file.

      self.workdir = config.castdir
      self.castdir = os.path.join(self.workdir,self.name)
      if not os.path.isdir(self.castdir): os.makedirs(self.castdir)

   def __del__( self ):
      if self.log.getEffectiveLevel() != logging.DEBUG:
         for f in self.flist:
            try: os.remove(f) #; self.log.debug('delete: %s'%f)
            except: self.log.warn('deleting error: '+f+' is not exist.')

   def find( self, dtime, retry=0 ):
      """get recent file name of SST"""
      if retry > self.maxRetry: return dtime,''

      fpattern = os.path.join(self.obsdir\
                ,'*'+dtime.strftime("%Y%m%d")+self.pattern)
      # pattern = "/op1/nwp/prep/ADEX/DCOD/DANR/*20150228*OSTIA*.nc.bz2"
      fname = glob.glob(fpattern); nFile = len(fname)
      if nFile == 1:
         return dtime, fname[0] # array to str
      elif nFile >  1: # latest file
         return dtime, max(glob.iglob(fpattern),key=os.path.getctime)
      else:
         return self.find(dtime-datetime.timedelta(days=1),retry+1)

   def get( self, mostRecentF, fname='sst_input.nc' ):
      fName, fExt = os.path.splitext(mostRecentF) # '/a/b/c.nc', '.bz2'
      if not os.path.isdir(self.castdir): os.makedirs(self.castdir)
      ofname = os.path.join(self.castdir,fname)
      if fExt == '.bz2':      # Unzip the file by bunzip2
         if os.path.isfile(ofname):
            os.remove(ofname) # sst_input.nc
            self.log.info(ofname+' is removed for renewal.')

         with open(ofname,'wb') as unfile,\
          bz2.BZ2File(mostRecentF,'rb') as bzfile:
            for data in iter(lambda : bzfile.read(100 * 1024), b''):
               unfile.write(data)
         self.log.info('bunzip2 '+mostRecentF+' to '+fname)
         self.flist.append(ofname) # to delete when finished
      else:
         self.log.error('Could not find any '+self.pattern)
         ofname = ''

      return ofname

   def run( self, fHeader='OSTIA_SST4WISE' ):
      # Find recent file name recusively and update the time
      dtime, mostRecentF = self.find(self.dtime)
      self.dtime = dtime
      if mostRecentF == '':
         self.log.error('not exist SST file - '+self.pattern)
         return ''

      # Get OSTIA SST
      if self.get(mostRecentF) == '': return '' # get OSTIA SST and update time.

      ukmo_sst = os.path.join(self.castdir,'UKMO_SST')
      if os.path.isfile(ukmo_sst):
         os.remove(ukmo_sst) # UKMO_SST
         self.log.info(ukmo_sst+' is removed before running '+self.sst_x)
      if runProcess(self.sst_x,self.castdir,self.log,timeoutS=600): return '' # if error.
      else:
         fSST = os.path.join(self.castdir,fHeader+self.dtime.strftime(".%Y%m%d"))
         os.rename(ukmo_sst,fSST)
         self.log.info('rename '+ukmo_sst+' to '+fSST)
         #self.flist.append(fSST) # to delete when finished

      self.log.info('[1.5] wr_mkbg_prep_usst SUCESSFUL FINISHED')
      return fSST

class WiseDA(object): # Observation Class
   """Data Assimilation Class
      KLAPS 05KM ANALYSIS ALL JOB
   """
   def __init__( self, config, timer, logger ):
      self.config = config
      self.timer  = timer
      self.logger = logger
      self.log    = logger.add(self.config.logdir,'WiseDA',self.config.logLevel)
      self.flist = []

      # Link static directory for BG
      srcF = os.path.join(self.config.datadir,'static')
      dstF = os.path.join(self.config.castdir,'static')
      symLink(srcF,dstF,self.log)

      # Create 'static/obs_driver.nl' for AWS, AMEDAS, LGT, MTAR
      nmlFile = os.path.join(self.config.castdir,'static/obs_driver.nl')
      with open(nmlFile+'.pre','r') as f: namelist = f.read() # obs_driver.nl.pre
      context = {"workdir":self.config.castdir}
      with open(nmlFile,'w') as f: f.write(namelist.format(**context))
      self.flist.append(nmlFile)

      # Create 'static/nest7grid.parms' for KWPF
      nmlFile = os.path.join(self.config.castdir,'static/nest7grid.parms')
      with open(nmlFile+'.pre','r') as f: namelist = f.read() # nest7grid.parms.pre
      context = {'datadir':self.config.datadir, 'workdir':self.config.castdir}
      with open(nmlFile,'w') as f: f.write(namelist.format(**context))
      self.flist.append(nmlFile)

      self.lfmpost_x  = os.path.join(self.config.bindir,'lfmpost_wrfv3.exe2')

      self.analysis_x = (os.path.join(self.config.bindir,'Analysis/su01_anal_wind.exe')\
                        ,os.path.join(self.config.bindir,'Analysis/su01_anal_lsfc.exe')\
                        ,os.path.join(self.config.bindir,'Analysis/su01_anal_temp.exe')\
                        ,os.path.join(self.config.bindir,'Analysis/su01_anal_mosc_radr.exe')\
                        ,os.path.join(self.config.bindir,'Analysis/su01_anal_clod_radr_ref2.exe')\
                        ,os.path.join(self.config.bindir,'Analysis/su01_anal_humd.exe')\
                        ,os.path.join(self.config.bindir,'Analysis/su01_anal_derv_tun2.exe')\
                        ,os.path.join(self.config.bindir,'Analysis/su01_anal_accm.exe')\
                        ,os.path.join(self.config.bindir,'Analysis/su01_anal_lsm5.exe'))
      self.wfo_post_pl = os.path.join(self.config.bindir,'etc/wfo_post.pl')
      self.qbal_x = os.path.join(self.config.bindir,'Analysis/su01_anal_qbal.exe')
      self.prep_x = os.path.join(self.config.bindir,'Analysis/su01_anal_prep.exe')

   def __del__( self ):
      if self.log.getEffectiveLevel() != logging.DEBUG:
         for f in self.flist:
            try: os.remove(f) #; self.log.debug('delete: %s'%f)
            except: self.log.warn('deleting error: '+f+' is not exist.')

         # Create empty core & dummy files if exist.
         coredummy = glob.glob(self.config.castdir+'/core*')\
                    +glob.glob(self.config.castdir+'/dummy*')
         for f in coredummy:
            t = os.path.getctime(f) # save file timestamp
            open(f,'w').close() # remove and create empty file
            os.utime(f,(t,t))   # restore file timestamp

   def makeInit( self, iFcstHour=6 ):
      """Make Init for 1km res.
         INFILE: wrfprd/suf5_lc01_guess_f??h_f??m from previous simulation
         OUTFILE: wrfpre/??.pcp
                  lapsprd/fsf/wrf/YYJJJHHMMHHMM.fsf # 2D
                  lapsprd/fua/wrf/YYJJJHHMMHHMM.fua # 3D
      """
      # Note) preStime is previous simulation time. for example -1 hour
      preStime = self.timer.UTC - datetime.timedelta(minutes=self.timer.runIntervalM)
      #preEtime = preStime + datetime.timedelta(hours=iFcstHour)

      lapsprd   = os.path.join(self.config.castdir,'lapsprd')
      wrfpre    = os.path.join(self.config.castdir,'wrfpre')
      preWrfprd = os.path.join(self.config.workdir,preStime.strftime("%y%m%d/%H%M/wrfprd"))

      # Skip analysis process if it is first time simulation.
      if not os.path.isdir(preWrfprd):
         self.log.warn("No previous wrfprd(%s). It will try pre-previous time."%preWrfprd)
         preStime = preStime - datetime.timedelta(minutes=self.timer.runIntervalM)
         preWrfprd = os.path.join(self.config.workdir,preStime.strftime("%y%m%d/%H%M/wrfprd"))
         if not os.path.isdir(preWrfprd):
            self.log.warn("No pre-previous wrfprd(%s). It may be first time simulation."%preWrfprd)
            return True

      ## Link static directory for lfmpost : '../static' in lmfutil.f90
      #staticDir = os.path.join(self.config.castdir,'static')
      #if not os.path.exists(staticDir):
      #   symLink(os.path.join(self.datadir,'static'),staticDir,self.log)

      # run LFMPOST 1km as bdy
      # OUT: wrfpre/??????.pcp & lapsprd/{fsf,fua}/wrf/YYJJJHHMMHHMM.{fsf,fua} # 2D & 3D
      env = {'NETCDF':self.config.netcdf\
            ,'PATH'  :os.path.join(self.config.netcdf,'bin')\
            ,'LD_LIBRARY_PATH':os.path.join(self.config.netcdf,'lib')\
            ,'LAPS_DATA_ROOT':self.config.castdir} # the root directory of 'wrfpre'
            #,'LAPS_DATA':os.path.join(self.config.castdir,'lapsprd/lapsprep/wps')}
      num = 0; pattern = 'suf5_lc01_guess_f??h_f??m'
      proc = [] # For run in parallel.
      for preWrffile in sorted(glob.glob(os.path.join(preWrfprd,pattern))):
         wrffile = os.path.join(wrfpre,os.path.basename(preWrffile))
         symLink(preWrffile,wrffile,self.log)
         # Usage: lfmpost.exe 'model type' 'filename' 'grid no' 'laps i4time'
         #                    'fcst time (sec)' 'laps_data_root'
         cmd = self.lfmpost_x+' wrf '+wrffile+' 1 '\
              +preStime.strftime("%Y%m%d%H ")+str(num)+' '+self.config.castdir
         p = Process(target=runProcess,args=(cmd,wrfpre,wrffile,env,None,600))
         #p = Process(target=runProcess,args=(cmd,wrfpre,self.log,{},None,600))
         p.start(); proc.append(p)
         self.log.info('running: '+cmd)
         if num != 0: # except first file for check later.
            self.flist.append(wrffile+'.out')
            self.flist.append(wrffile+'.err')

         num += self.timer.histIntervalM
      for p in proc: p.join() # Wait for the worker processes to exit.

      # 0.pcp, 3600.pcp, ..., 21600.pcp
      for f in glob.glob(os.path.join(wrfpre,'*.pcp')): self.flist.append(f)
      # lapsprd/{fsf,fua}/wrf/YYJJJHHMMHHMM.{fsf,fua} # 2D & 3D
      for f in glob.glob(os.path.join(lapsprd,'fsf/wrf/*.fsf')): self.flist.append(f)
      for f in glob.glob(os.path.join(lapsprd,'fua/wrf/*.fua')): self.flist.append(f)

      return False

   def merge( self ):
      """KLAPS 05KM LSO DATA MERGE
         ${SU01DAOU}/lapsprd/lso
      """
      self.log.info('start merging')

      merge_x = os.path.join(self.config.bindir,"wr_count_merge_lso2.x")

      # SURFACE DATA MERGE PRECESS(0:unused, 1:used)
      jobString = \
"""0                                           !! gso [GTS]
1                                              !! aws
0                                              !! qsc [QSCAT]
0                                              !! buy [BUOY]
1                                              !! ads [AMEDAS]
0                                              !! mar [METAR]
1                                              !! met [KMETAR]
0                                              !! iad [IEODO]
"""
      env = {"LAPS_DATA_ROOT":self.config.castdir}
      #       "LAPS_DATA":os.path.join(self.config.castdir,"lapsprd/lapsprep/wps")}
      runProcess(merge_x,self.config.castdir,self.log,env,jobString,600)

   def analysis( self ):
      """KLAPS 05KM ANALYSIS"""
      ifname = os.path.join(self.config.castdir,'static/nest7grid.parms.pre')
      ofname = os.path.join(self.config.castdir,'static/nest7grid.parms')
      context = {"datadir":self.config.datadir, "workdir":self.config.castdir}
      copyReplace(context,ifname,ofname,self.log)

      env = {'NETCDF':self.config.netcdf\
            ,'PATH'  :os.path.join(self.config.netcdf,'bin')\
            ,'LD_LIBRARY_PATH':os.path.join(self.config.netcdf,'lib')\
            ,'LAPS_DATA_ROOT':self.config.castdir\
            ,'GA_VV_TO_HEIGHT_RATIO_CU':'1.5'\
            ,'GA_VV_TO_HEIGHT_RATIO_SC':'0.11'\
            ,'GA_VV_FOR_ST'            :'0.017'}
      #      ,'LAPS_DATA':os.path.join(self.config.castdir,"lapsprd/lapsprep/wps")\
      #      ,'GA_VV_TO_HEIGHT_RATIO_CU':'3.0'\
      #      ,'GA_VV_TO_HEIGHT_RATIO_SC':'0.22'\
      #      ,'GA_VV_FOR_ST'            :'0.035'}
      # Lower to remove too strong rain cell at 2010.3.26 ###
      # Run su01_anal_wind.exe, su01_anal_lsfc.exe, su01_anal_temp.exe,
      #     su01_anal_mosc_radr.exe, su01_anal_clod_radr_ref2.exe, su01_anal_humd.exe
      #     su01_anal_derv_tun2.exe, su01_anal_accm.exe, su01_anal_lsm5.exe
      for x in self.analysis_x:
         self.log.info('##### Running : '+os.path.basename(x)+' #####')
         runProcess(x,self.config.castdir,self.log,env,timeoutS=600)

      # MAKE BIGFILE AND FTP TO FAS
      bigDir = os.path.join(self.config.castdir,'lapsprd/bigfile')
      # Copy 'cdl/template' to 'lapsprd/bigfile/.'
      dstF = os.path.join(bigDir,'template')
      if not os.path.isfile(dstF):
         srcF = os.path.join(self.config.datadir,'cdl/template')
         try: shutil.copyfile(srcF,dstF)
         except: raise RuntimeError
      # Link 'cdl/laps.cdl' to 'lapsprd/bigfile/.'
      symLink(os.path.join(self.config.castdir,'cdl/laps.cdl')\
             ,os.path.join(bigDir,'laps.cdl'),self.log)

      #for f in glob.glob(os.path.join(bigDir,'*_????')): os.remove(f)

      env = {'NETCDF':self.config.netcdf\
            ,'PATH'  :os.path.join(self.config.netcdf,'bin')\
            ,'LD_LIBRARY_PATH':os.path.join(self.config.netcdf,'lib')\
            ,'LAPSROOT':self.config.bindir\
            ,'LAPS_DATA_ROOT':self.config.castdir}
            #,'LAPS_DATA':os.path.join(self.config.castdir,'lapsprd/lapsprep/wps')}
      cmd = self.wfo_post_pl+' '+'-w'+' '+self.config.bindir+' '+self.config.castdir
      runProcess(cmd,bigDir,self.log,env,timeoutS=600)
      for f in glob.glob(os.path.join(bigDir,'*_????')): self.flist.append(f)

      # BALANCE AND MAKE ANAL FIELD
      runProcess(self.qbal_x,self.config.castdir,self.log,env,timeoutS=600)
      runProcess(self.prep_x,self.config.castdir,self.log,env,timeoutS=600)

      #lapsDir = os.path.join(self.config.castdir,self.timer.UTC.strftime("%Y%m%d"))
      lapsDir = os.path.join(self.config.castdir,'WiseDA')
      if not os.path.isdir(lapsDir): os.makedirs(lapsDir)
      srcF=os.path.join(self.config.castdir,'lapsprd/lapsprep/wps'\
          ,self.timer.UTC.strftime("LAPS:%Y-%m-%d_%H"))
      if os.path.isfile(srcF):
         #dstF=os.path.join(self.config.castdir,'lapsprd/lapsprep/wps'\
         #    ,self.timer.UTC.strftime("LAPS:%Y-%m-%d_%H:%M"))
         dstF=os.path.join(lapsDir,self.timer.UTC.strftime("LAPS:%Y-%m-%d_%H:%M"))
         try: shutil.copyfile(srcF,dstF)
         except: raise RuntimeError
         #if self.timer.UTC.minute != 0: os.rename(srcF,srcF+':00')
         #symLink(dstF,os.path.join(lapsDir,os.path.basename(dstF)),self.log)

         self.flist.append(srcF)

   def run( self ):
      if self.makeInit(): return False # return False if makeInist() returns True.

      # Get, decode, and ingest of observation data
      MTSAT  = KmaMTSAT(self.config,self.timer,self.logger);  MTSAT.run()
      AWS    = KmaAWS(self.config,self.timer,self.logger);    AWS.run()
      LGT    = KmaLGT(self.config,self.timer,self.logger);    LGT.run()
      AMEDAS = KmaAMEDAS(self.config,self.timer,self.logger); AMEDAS.run()
      KWPF   = KmaWPF(self.config,self.timer,self.logger);    KWPF.run()
      MTAR   = KmaMTAR(self.config,self.timer,self.logger);   MTAR.run()
      #GTS   = KmaGTS(self.config,self.timer,self.logger);    GTS.run()
      AMDAR  = KmaAMDAR(self.config,self.timer,self.logger);  AMDAR.run()
      RADAR  = KmaRADAR(self.config,self.timer,self.logger);  RADAR.run()

      #proc = [] # Run functions in parallel
      #for fn in [MTSAT.run,AWS.run,AMEDAS.run,LGT.run\
      #          ,KWPF.run,MTAR.run,AMDAR.run,RADAR.run]:
      #   p = Process(target=fn)
      #   p.start(); proc.append(p)
      #for p in proc: p.join()

      self.merge()    # KLAPS 05KM LSO DATA MERGE
      self.analysis() # KLAPS 05KM ANALYSIS

      return False

