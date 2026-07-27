from setuptools import setup, find_packages

setup(
    name="aera-oxygen",
    version="2.1.0",
    packages=find_packages(include=["aera_oxygen", "aera_oxygen.*"]),
    author="Yong-Yub Kim / modified from AERA",
    description="Modified AERA algorithm for oxygen-based adaptive emissions",
    license="CC BY-NC-SA 4.0",
    keywords="AERA, oxygen, climate change, mitigation",
    package_data={
        "aera_oxygen": ["data/*.dat"],
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
