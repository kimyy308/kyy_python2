#!/usr/bin/env python3
# -*- coding: cp949 -*-
"""
SYNOPSIS
       WiseR system resource configuration file

EXAMPLES
       cfgWiseR.py ???
"""
# Created: May. 2015

from configparser import ConfigParser,ExtendedInterpolation

cfg = ConfigParser(interpolation=ExtendedInterpolation())

section = 'COMMON'; cfg.add_section(section) # Common variables ###############
cfg.set(section,'cast','Hcast')        # F = Forecast, H = hindcast
cfg.set(section,'time','201502280238') # used if cast = H or Hcast

section = 'WISE'; cfg.add_section(section) ####################################
cfg.set(section,'WiseR.1a','/data/kma/wise/models/WISE/WiseR.1a')
cfg.set(section,'bindir','bin')
cfg.set(section,'modldir','model')
cfg.set(section,'utildir','util')
cfg.set(section,'datadir','data')
cfg.set(section,'diagdir','diag')
cfg.set(section,'workdir','work')

section = 'COMIS3'; cfg.add_section(section) #################################
cfg.set(section,'ipaddr' ,'172.20.159.21')
cfg.set(section,'userid' ,'frldata')
cfg.set(section,'passwd' ,'frldata!@#')
cfg.set(section,'netrc'  ,'True')
cfg.set(section,'verbose','False')
cfg.set(section,'MTSAT'  ,'/DATA/SAT/MTSAT')
cfg.set(section,'AWS'    ,'/DATA/AWS/AWSDB')
cfg.set(section,'AMEDAS' ,'/DATA/AWS/AMEDAS')
cfg.set(section,'LGT'    ,'/DATA/LGT/KMA')
cfg.set(section,'WPF'    ,'/DATA/UPP/WPF/PRO')
cfg.set(section,'MTAR'   ,'/DATA/AIR')
cfg.set(section,'GTS'    ,'???')
cfg.set(section,'AMDAR'  ,'/DATA/GTS/BULL/BUFR')
cfg.set(section,'RADAR'  ,'/DATA/RDR/QCD')
cfg.set(section,'NWP'    ,'/C4N_DATA/DATA/NWP')

section = 'HAEBIT'; cfg.add_section(section) ##################################
cfg.set(section,'haebit1','210.125.45.35')
cfg.set(section,'haebit2','210.125.45.36')
cfg.set(section,'haebit3','210.125.45.37')
cfg.set(section,'haebit4','210.125.45.38')
cfg.set(section,'sdb'    ,'210.125.45.39')
cfg.set(section,'haebit9','210.125.45.76')
cfg.set(section,'partner','210.125.45.76')
cfg.set(section,'userid' ,'skim')
cfg.set(section,'passwd' ,'skim123')
cfg.set(section,'COMIS3' ,'/data/urban/data/KMA/COMIS3')

section = 'WISE_WEB'; cfg.add_section(section) ################################
cfg.set(section,'ipaddr','1.225.165.188')
cfg.set(section,'userid','skim')
cfg.set(section,'COMIS3','/home/data/KMA/COMIS3')

section = 'GTSR'; cfg.add_section(section) ####################################
gtsrhome = '/op1/nwp/prep/ADEX/GTSR' #cfg.set('GTSRHOME',gtsrhome)
cfg.set(section,'gtsrawsd',gtsrhome+'/AWSD')
cfg.set(section,'gtsrdaio',gtsrhome+'/DAIO')
cfg.set(section,'gtsrshel',gtsrhome+'/SHEL')
cfg.set(section,'gtsr_src',gtsrhome+'/.SRC')
cfg.set(section,'gtsrdaba',gtsrhome+'/DABA')
cfg.set(section,'gtsrdasv',gtsrhome+'/DASV')
cfg.set(section,'gtsrdain',gtsrhome+'/DAIN')
cfg.set(section,'gtsrdaou',gtsrhome+'/DAOU')
cfg.set(section,'gtsrsend',gtsrhome+'/SEND')
cfg.set(section,'gtsrtemp',gtsrhome+'/TEMP')
cfg.set(section,'gtsrlogo',gtsrhome+'/LOGO')
cfg.set(section,'gtsrdanr',gtsrhome+'/DANR')
cfg.set(section,'gtsrexet',gtsrhome+'/EXET')

section = 'DCOD'; cfg.add_section(section) ####################################
dcodhome = '/op1/nwp/prep/ADEX/DCOD' #cfg.set('DCOD','dcodhome',dcodhome)
cfg.set(section,'dcod_src',dcodhome+'/.SRC')
cfg.set(section,'dcodmake',dcodhome+'/MAKE')
cfg.set(section,'dcodshel',dcodhome+'/SHEL')
cfg.set(section,'dcodexet',dcodhome+'/EXET')
cfg.set(section,'dcodlogo',dcodhome+'/LOGO')
cfg.set(section,'dcoddaba',dcodhome+'/DABA')
cfg.set(section,'dcoddain',dcodhome+'/DAIN')
cfg.set(section,'dcoddaio',dcodhome+'/DAIO')
cfg.set(section,'dcoddaou',dcodhome+'/DAOU')
cfg.set(section,'dcoddagr',dcodhome+'/DAGR')
cfg.set(section,'dcoddanr',dcodhome+'/DANR')
cfg.set(section,'dcodgifd',dcodhome+'/GIFD')
cfg.set(section,'dcoddasv',dcodhome+'/DASV')

section = 'KLAPS'; cfg.add_section(section) ###################################
klapshome = '/home/wise/models/KLAPS/klaps3.0.1'
            #cfg.set('klapshome','klapshome)
cfg.set(section,'qlogpath',klapshome+'/LOGO')
cfg.set(section,'kl05home',klapshome+'/fcst/ANAL/KL05/X512')
cfg.set(section,'klfsdain',klapshome+'/fcst/MODL/KLFS/X512/DAIN')
cfg.set(section,'kl05dagr',klapshome+'/DAGR')
cfg.set(section,'kl05exet',klapshome+'/EXET')
cfg.set(section,'klbgdaou',klapshome+'/fcst/MODL/KLBG/X512/DAOU')
cfg.set(section,'klbglogo',klapshome+'/fcst/MODL/KLBG/X512/LOGO')
cfg.set(section,'kl05daba',klapshome+'/DABA')
cfg.set(section,'kl05daou',klapshome+'/DAOU')
cfg.set(section,'kl05shel',klapshome+'/SHEL')
cfg.set(section,'klfsshel',klapshome+'/fcst/MODL/KLFS/X512/SHEL')
cfg.set(section,'kl05gifd',klapshome+'/GIFD')
cfg.set(section,'kl05dain',klapshome+'/DAIN')
cfg.set(section,'kl05daio',klapshome+'/DAIO')
cfg.set(section,'laps_data_root',klapshome+'/DAOU')
cfg.set(section,'kl05logo',klapshome+'s/LOGO')

section = 'UTIL'; cfg.add_section(section) ####################################
cfg.set(section,'utilexet','/home/wise/models/KLAPS/klaps3.0.1/EXET')

section = 'PREPOST'; cfg.add_section(section) #################################
cfg.set(section,'grid'     ,'X512')
cfg.set(section,'prepost'  ,'cirrus2')
cfg.set(section,'work_time','erly')

section = 'OPERATION'; cfg.add_section(section) ###############################
cfg.set(section,'supercom','haedam')
cfg.set(section,'prepost' ,'cirrus2')
cfg.set(section,'quename' ,'normal')

section = 'hosts'; cfg.add_section(section) ###################################
cfg.set(section,'ncomis1'  ,'190.1.5.220')
cfg.set(section,'nwpwss'   ,'190.1.20.55')
cfg.set(section,'satellite','190.1.12.150')
cfg.set(section,'radar'    ,'190.1.17.178')
cfg.set(section,'nwpsea1'  ,'190.1.5.185')
cfg.set(section,'prepost2' ,'190.1.22.19')
cfg.set(section,'prepost1' ,'190.1.22.17')
cfg.set(section,'fcstlp'   ,'190.1.12.210')
cfg.set(section,'wdtsvr'   ,'190.1.12.49')
cfg.set(section,'ncomis'   ,'190.1.20.40')
cfg.set(section,'comis3_laps','172.20.159.21')
cfg.set(section,'filer'    ,'172.19.194.32')
cfg.set(section,'nwpfep'   ,'190.1.21.50')
cfg.set(section,'taps3'    ,'172.25.151.100')
cfg.set(section,'taps2'    ,'190.1.17.153')
cfg.set(section,'taps1'    ,'190.1.17.150')
cfg.set(section,'kmaweb'   ,'203.247.66.206')
cfg.set(section,'comis3'   ,'172.20.159.11')
cfg.set(section,'nradar'   ,'172.20.157.11')
cfg.set(section,'nwpsea'   ,'190.1.20.20')
cfg.set(section,'nwpdas'   ,'190.1.20.56')
cfg.set(section,'comis3_n' ,'comis_rcv1')
cfg.set(section,'qcradar'  ,'172.20.157.12')


# Writing configuration file to 'example.cfg'
with open('example.cfg','wb') as configfile:
   cfg.write(configfile)

check = SafeConfigParser()
check.read('example.cfg')
print check.get('DCOD','dcoddanr')

