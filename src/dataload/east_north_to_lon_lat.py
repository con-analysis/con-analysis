import pyproj

# Convert eastings, northing to lat, long
crs_british = pyproj.Proj(init='EPSG:27700')
crs_wgs84 = pyproj.Proj(init='EPSG:4326')
def en_to_lon_lat(row):
    lon, lat = pyproj.transform(crs_british, crs_wgs84, row['x'], row['y'])
    return lat, lon