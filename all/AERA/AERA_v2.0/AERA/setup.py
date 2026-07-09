from setuptools import setup, find_packages

setup(
    name="aera-oa",
    version="2.0.0",
    packages=find_packages(),
    author="Yong-Yub Kim / modified from AERA",
    description="Modified AERA algorithm for ocean acidification / oxygen targets",
    license="CC BY-NC-SA 4.0",
    keywords="research, climate change, mitigation, AERA, ocean biogeochemistry",
    package_data={
        "aera_oa": ["data/*.dat"],
    },
    include_package_data=True,
    install_requires=[
        "numpy",
        "pandas",
        "netCDF4",
        "scipy",
        "dask[complete]",
        "xarray",
    ],
)
