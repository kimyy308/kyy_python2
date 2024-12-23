# To configure, read and preprocess CESM2 Large ensemble, ODA, ADA and hindcast files in Aleph
# Created 11-Sep-2024 by Yong-Yub Kim, kimyy308@pusan.ac.kr

import warnings
warnings.simplefilter(action='ignore')
import subprocess
import re as re_mod
import pandas as pd
from pprint import pprint
import numpy as np
import xarray as xr
import cftime

class CESM2_config:
    def __init__(self):
        self.compset = 'b'
        self.codebase = 'e21'
        self.resol = 'f09_g17'
        
    def list(self):
        # pprint(self.__dict__, width=1)
        filtered_dict = {k: v for k, v in self.__dict__.items() if k not in ('ODA_file_list', 'LE_file_list', 'ADA_file_list', 'OBS_file_list')}
        pprint(filtered_dict, width=1)

    def setvar(self, var):
        self.var=var
        df = pd.read_csv('/mnt/lustre/proj/earth.system.predictability/LENS2/LENS2_output_200129.csv', header=None, on_bad_lines='skip')
        #filtered_row = df[df[3].str.contains(self.var, regex=True, na=False)]
        filtered_row = df[df[3].str.match(self.var)]
        self.comp = filtered_row.iloc[0, 0]      
        self.long_name = ''.join(filtered_row.iloc[0, 5])
        self.unit = filtered_row.iloc[0, 6]       
        self.dimension = filtered_row.iloc[0, 7]  
        self.ndim = len(self.dimension.split())
        self.tfreq='month_1'
        # set component
        if self.comp=='ocn':
            self.model='pop.h'
        elif self.comp=='ice':
            self.model='cice.h'
        elif self.comp=='atm':
            self.model='cam.h0'
        elif self.comp=='lnd':
            self.model='clm2.h0'

    def HCST_path_load(self, var):
        # HCST path check
        self.HCST_rootdir = '/mnt/lustre/proj/earth.system.predictability/HCST_EXP_timeseries/archive'
        command='ls ' + self.HCST_rootdir + '| grep f09_g17 | cut -d ''.'' -f 3-4'
        HCST_members_raw = subprocess.check_output(command, shell=True, text=True)
        self.HCST_members= [entry for entry in HCST_members_raw.split('\n') if entry]
        self.HCST_ensembles = [ens for ens in range(len(self.HCST_members))]
        self.HCST_file_list=[]
        HCST_file_list=[]
        
        HCST_iy_files = []
        for iyear in range(self.year_s, self.year_e+1):
            HCST_ens_files=[]
            for member in self.HCST_members:
                HCST_files=[]    
                HCST_casename_M=self.resol + '.hcst.' + member
                HCST_casename = HCST_casename_M + '_i' + str(iyear)
                # command='ls ' + self.HCST_rootdir + '/*' + member + '*/' + self.comp + '/proc/tseries/' + self.tfreq + '| grep \'\.' + self.var + '\.\''
                # print(command)
                HCST_files = (
                    self.HCST_rootdir + '/' + HCST_casename_M + '/' + HCST_casename + '/' +
                    self.comp + '/proc/tseries/' + self.tfreq + '/' + HCST_casename + '.' +
                    self.model + '.' + self.var + '.' + str(iyear) + '01' + '-' +
                    str(iyear + 4) + '12' + '.nc'
                )
                HCST_ens_files.append(HCST_files)
            HCST_iy_files.append(HCST_ens_files)
        self.HCST_file_list=HCST_iy_files
    
    def ODA_path_load(self, var):
        # ODA path check
        self.ODA_rootdir = '/mnt/lustre/proj/earth.system.predictability/ASSM_EXP_timeseries/archive'
        scenarios = r'\.(BHISTsmbb|BSSP370smbb)\.'
        command='ls ' + self.ODA_rootdir + '| grep BHISTsmbb | cut -d ''.'' -f 5-7'
        ODA_members_raw = subprocess.check_output(command, shell=True, text=True)
        self.ODA_members= [entry for entry in ODA_members_raw.split('\n') if entry]
        self.ODA_ensembles = [ens for ens in range(len(self.ODA_members))]
        ODA_file_list=[]
        ODA_ens_files=[]
        for member in self.ODA_members :
            ODA_files=[]    
            command='ls ' + self.ODA_rootdir + '/*' + member + '*/' + self.comp + '/proc/tseries/' + self.tfreq + '| grep \'\.' + self.var + '\.\''
            # print(command)
            ODA_files_raw = subprocess.check_output(command, shell=True, text=True)
            ODA_files= [entry for entry in ODA_files_raw.split('\n') if entry]
            ODA_files = sorted(ODA_files)
            # Filter the files based on your criteria
            ODA_filtered_files = []
            for fname in ODA_files:
                # print(fname)  # Debugging: print each file name
                match1 = re_mod.search(r'-(\d{4})12', fname)
                match2 = re_mod.search(r'\.(\d{4})01', fname)
                # Ensure the regex matches and then check the year range
                if match1 and match2:
                    year1 = int(match1.group(1))
                    year2 = int(match2.group(1))
                    if year1 >= self.year_s and year2 <= self.year_e:
                        scenario=re_mod.search(scenarios, fname).group(1)
                        fpath=self.ODA_rootdir + '/' + 'b.e21.' + scenario + '.' + self.resol + '.' + member + '/' + self.comp + '/proc/tseries/' + self.tfreq + '/' + fname
                        # print(fpath)
                        ODA_filtered_files.append(fpath)
            ODA_ens_files.append(ODA_filtered_files)
        ODA_file_list.append(ODA_ens_files)
        self.ODA_file_list=ODA_file_list

    def ADA_path_load(self, var):
        # ADA path check
        self.ADA_rootdir = '/mnt/lustre/proj/earth.system.predictability/ATM_TEST/EXP_ALL_timeseries/archive'
        scenarios = r'\.(BHISTsmbb|BSSP370smbb)\.'
        command='ls ' + self.ADA_rootdir + '| grep BHISTsmbb | cut -d ''.'' -f 5-7'
        ADA_members_raw = subprocess.check_output(command, shell=True, text=True)
        self.ADA_members= [entry for entry in ADA_members_raw.split('\n') if entry]
        self.ADA_ensembles = [ens for ens in range(len(self.ADA_members))]
        ADA_file_list=[]
        ADA_ens_files=[]
        for member in self.ADA_members :
            ADA_files=[]    
            command='ls ' + self.ADA_rootdir + '/*' + member + '*/' + self.comp + '/proc/tseries/' + self.tfreq + '| grep \'\.' + self.var + '\.\''
            # print(command)
            ADA_files_raw = subprocess.check_output(command, shell=True, text=True)
            ADA_files= [entry for entry in ADA_files_raw.split('\n') if entry]
            ADA_files = sorted(ADA_files)
            # Filter the files based on your criteria
            ADA_filtered_files = []
            for fname in ADA_files:
                # print(fname)  # Debugging: print each file name
                # match1 = re_mod.search(r'-(\d{4})12', fname)
                match1 = re_mod.search(r'-(\d{4})\d{2}.nc', fname)
                match2 = re_mod.search(r'\.(\d{4})01', fname)
                # Ensure the regex matches and then check the year range
                if match1 and match2:
                    year1 = int(match1.group(1))
                    year2 = int(match2.group(1))
                    if year1 >= self.year_s and year2 <= self.year_e:
                        scenario=re_mod.search(scenarios, fname).group(1)
                        fpath=self.ADA_rootdir + '/' + 'b.e21.' + scenario + '.' + self.resol + '.' + member + '/' + self.comp + '/proc/tseries/' + self.tfreq + '/' + fname
                        # print(fpath)
                        ADA_filtered_files.append(fpath)
            ADA_ens_files.append(ADA_filtered_files)
        ADA_file_list.append(ADA_ens_files)
        self.ADA_file_list=ADA_file_list

    def LE_path_load(self, var):
        # LE path check
        self.LE_rootdir = '/proj/jedwards/archive'
        scenarios = r'\.(BHISTsmbb|BSSP370smbb)\.'
        command='ls ' + self.LE_rootdir + '| grep BHISTsmbb | cut -d ''.'' -f 5-6'
        LE_members_raw = subprocess.check_output(command, shell=True, text=True)
        self.LE_members= [entry for entry in LE_members_raw.split('\n') if entry]
        self.LE_ensembles = [ens for ens in range(len(self.LE_members))]
        LE_file_list=[]
        LE_ens_files=[]
        for member in self.LE_members :
            LE_files=[]    
            command='ls ' + self.LE_rootdir + '/*' + member + '*/' + self.comp + '/proc/tseries/' + self.tfreq + '| grep \'\.' + self.var + '\.\''
            # print(command)
            LE_files_raw = subprocess.check_output(command, shell=True, text=True)
            LE_files= [entry for entry in LE_files_raw.split('\n') if entry]
            LE_files = sorted(LE_files)
            # Filter the files based on your criteria
            LE_filtered_files = []
            for fname in LE_files:
                # print(fname)  # Debugging: print each file name
                match1 = re_mod.search(r'-(\d{4})12', fname)
                match2 = re_mod.search(r'\.(\d{4})01', fname)
                # Ensure the regex matches and then check the year range
                if match1 and match2:
                    year1 = int(match1.group(1))
                    year2 = int(match2.group(1))
                    if year1 >= self.year_s and year2 <= self.year_e:
                        scenario=re_mod.search(scenarios, fname).group(1)
                        fpath=self.LE_rootdir + '/' + 'b.e21.' + scenario + '.' + self.resol + '.' + member + '/' + self.comp + '/proc/tseries/' + self.tfreq + '/' + fname
                        # print(fpath)
                        LE_filtered_files.append(fpath)
            LE_ens_files.append(LE_filtered_files)
        LE_file_list.append(LE_ens_files)
        self.LE_file_list=LE_file_list


    def OBS_path_load(self, var):
        # set varname in observation file
        self.OBS_rootdir='/mnt/lustre/proj/kimyy/Observation';
        match self.var:
            case 'SST':
                self.OBS_var='sst'
                self.OBS_dataname='ERSST'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'monthly_reg_' + self.model[:3]
            case 'PRECT':
                self.OBS_var='precip'
                self.OBS_dataname='GPCC'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'monthly_reg_' + self.model[:3]
            case 'RAIN':
                self.OBS_var='precip'  # GPCC
                self.OBS_dataname='GPCC'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'monthly_reg_' + self.model[:3]
            case 'PSL':
                self.OBS_var='msl'
                self.OBS_dataname='ERA5'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + self.OBS_var + '/' + 'monthly_reg_' + self.model[:3]
            case 'SOILWATER_10CM':
                self.OBS_var='SMsurf' #GLEAM
                self.OBS_dataname='GLEAM'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + self.OBS_var + '/' + 'monthly_reg_' + self.model[:3]
            case 'TWS':
                self.OBS_var='w'
                self.OBS_dataname='NOAA'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'TSW' + '/' + 'monthly_reg_' + self.model[:3]       
            case 'SSH':
                self.OBS_var='sla'
                self.OBS_dataname='CMEMS'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + self.OBS_var + '/' + 'monthly_reg_' + self.model[:3]  
            case 'TS':
                self.OBS_var='t2m'  #ERA5
                # self.OBS_dataname='ERA5'
                self.OBS_dataname='CRU_TS'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'monthly_reg_' + self.model[:3]  
            case 'TREFHT':
                self.OBS_var='t2m'  #ERA5
                # self.OBS_dataname='SMYLE'
                self.OBS_dataname='CRU_TS'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'monthly_reg_' + self.model[:3]  
            case 'sumChl':
                self.OBS_var='chlor_a'
                self.OBS_dataname='OC_CCI'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'monthly_reg_' + self.model[:3]
            case 'TLAI':
                self.OBS_var='LAI' # NOAA LAI
                self.OBS_dataname='NOAA'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + self.OBS_var + '/' + 'monthly_reg_' + self.model[:3]                  
            case 'FAREA_BURNED':
                self.OBS_var='burned_area'; # MODIS Fire_cci v5.1, AVHRR-LTDR
                # self.OBS_dataname='MODIS'
                self.OBS_dataname='AVHRR'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'AVHRR-LTDR' + '/' + 'monthly_reg_' + self.model[:3]                  
            case 'COL_FIRE_CLOSS':
                self.OBS_var='C' # GFED
                self.OBS_dataname='GFED'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'FIRE_CLOSS' + '/' + 'monthly_reg_' + self.model[:3]                                  
            case 'photoC_TOT_zint':
                self.OBS_var='PP' #Globcolour
                self.OBS_dataname='CMEMS'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'GlobColour' + '/' + 'monthly_reg_' + self.model[:3] + '/PP/'  
            case 'photoC_TOT_zint_100m':
                self.OBS_var='PP' #Globcolour
                self.OBS_dataname='CMEMS'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'GlobColour' + '/' + 'monthly_reg_' + self.model[:3] + '/PP/'  
            case 'photoC_NO3_TOT_zint_100m':
                self.OBS_var='PP' #Globcolour
                self.OBS_dataname='CMEMS'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'GlobColour' + '/' + 'monthly_reg_' + self.model[:3] + '/PP/'
            case 'GPP':
                self.OBS_var='GPP'
                self.OBS_dataname='VODCA2GPP'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'monthly_reg_' + self.model[:3]
            case 'FG_CO2':
                self.OBS_var='fgco2_smoothed'
                self.OBS_dataname='SOM-FFN'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'monthly_reg_' + self.model[:3]
            case 'DpCO2':
                self.OBS_var='dco2'
                self.OBS_dataname='SOM-FFN'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'monthly_reg_' + self.model[:3]
            case 'DpCO2_ALT_CO2':
                self.OBS_var='dco2'
                self.OBS_dataname='SOM-FFN'
                self.OBS_mondir= self.OBS_rootdir + '/' +  self.OBS_dataname + '/' + 'monthly_reg_' + self.model[:3]
            case _:
                self.OBS_var='nan'

        
        OBS_file_list=[]
        OBS_ens_files=[]
        OBS_files=[]    
        command='ls ' + self.OBS_mondir + '/*reg_*.nc'
        # print(command)
        OBS_files_raw = subprocess.check_output(command, shell=True, text=True)
        OBS_files= [entry for entry in OBS_files_raw.split('\n') if entry]
        OBS_files = sorted(OBS_files)
        # Filter the files based on your criteria
        OBS_filtered_files = []
        for fname in OBS_files:
            # print(fname)  # Debugging: print each file name
            match1 = re_mod.search(r'.(\d{4})', fname)
            # Ensure the regex matches and then check the year range
            if match1:
                year1 = int(match1.group(1))
                if year1 >= self.year_s and year1 <= self.year_e:
                    fpath= fname
                    # print(fpath)
                    OBS_filtered_files.append(fpath)
        OBS_ens_files.append(OBS_filtered_files)
        OBS_file_list.append(OBS_ens_files)
        self.OBS_file_list=OBS_file_list

    

# usage:
# abcdd=CESM2_config()
# abcdd.year_s=1960
# abcdd.year_e=2020
# abcdd.setvar('SST')
# abcdd.ODA_path_load(abcdd.var)
# abcdd.LE_path_load(abcdd.var)
# abcdd.list()
# abcdd.LE_file_list