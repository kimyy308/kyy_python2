"""
This module implements the Adaptive Emission Reduction 
Approach (AERA) developed by Terhaar et al. (in prep.). 
The AERA iteratively calculates every 5 years CO2-fe 
emissions , which allow to stabilize global earth 
temperature at a chosen temperature target.
"""

from aera_oxygen.core import get_adaptive_emissions
from aera_oxygen.utils import get_base_df
