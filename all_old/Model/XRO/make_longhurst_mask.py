#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import geopandas as gpd
import numpy as np
import xarray as xr
import rasterio
from rasterio import features

def main():

    print("Reading shapefile...")

    # --------------------------------------------------------
    # 1) Read Longhurst shapefile
    # --------------------------------------------------------
    shp_path = "/proj/kimyy/Observation/longhurst_v4_2010/Longhurst_world_v4_2010.shp"
    gdf = gpd.read_file(shp_path)

    # Map province codes to integer IDs
    prov_codes = gdf["ProvCode"].tolist()
    prov_to_int = {code: i+1 for i, code in enumerate(prov_codes)}  # 1-based
    int_to_prov = {v: k for k, v in prov_to_int.items()}

    print("Number of provinces:", len(prov_codes))

    # --------------------------------------------------------
    # 2) Define 1° × 1° global grid
    # --------------------------------------------------------
    lon_res = 1.0
    lat_res = 1.0

    lons = np.arange(-180, 180, lon_res)
    lats = np.arange(90, -90, -lat_res)

    nlon = len(lons)
    nlat = len(lats)

    transform = rasterio.transform.from_origin(-180, 90, lon_res, lat_res)

    # --------------------------------------------------------
    # 3) Rasterize polygons
    # --------------------------------------------------------
    print("Rasterizing polygons...")

    shapes = [
        (geom, prov_to_int[code])
        for geom, code in zip(gdf.geometry, gdf["ProvCode"])
    ]

    raster = features.rasterize(
        shapes=shapes,
        out_shape=(nlat, nlon),
        transform=transform,
        fill=0,
        dtype="int32"
    )

    # --------------------------------------------------------
    # 4) Create xarray Dataset
    # --------------------------------------------------------
    print("Converting to xarray...")

    ds = xr.Dataset(
        {"Longhurst_mask": (("lat", "lon"), raster)},
        coords={"lat": lats, "lon": lons},
        attrs={
            "description": "Longhurst Biogeochemical Provinces (1-degree rasterized)",
            "source": shp_path,
            "province_code_mapping": str(int_to_prov)
        }
    )

    # --------------------------------------------------------
    # 5) Save to NetCDF
    # --------------------------------------------------------
    outfile = "/proj/kimyy/Observation/longhurst_v4_2010/Longhurst_1deg_mask.nc"
    ds.to_netcdf(outfile)

    print("DONE! Saved:", outfile)


# Run main() when executed in terminal
if __name__ == "__main__":
    main()

