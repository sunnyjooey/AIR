from rasterio.transform import Affine

rasterize_map = {
    "roads": {"attr_field": "class_id", "fname_out": "roads.tif"},
    "rivers": {"attr_field": "navigable", "fname_out": "rivers.tif"},
    "boundary": {"attr_field": "OBJECTID", "fname_out": "boundary.tif"},
}

speed_map = {
    "roads": {1: 110.0, 2: 90.0, 3: 90.0, 4: 90.0, 5: 90.0, 6: 50.0, 7: 24.3},
    "rivers": {1: 10.0, 2: 10, 0: 0.0},
    "landcover": {
        1: 3.24,
        2: 3.0,
        3: 4.86,
        4: 2.5,
        5: 2.0,
        6: 2.0,
        7: 3,
        8: 5.0,
        10: 2.0,
    },
    "boundary": {1: 1},
}

road_attrs = {
    "Minor": ["#A2A2A2", 1.15],
    "Primary": ["#FF4100", 1.30],
    "Railway": ["black", 1.5],
    "Residential": ["#ACC1F0", 1.10],
    "Secondary": ["#F1A220", 1.25],
    "Tertiary": ["#FDFD1F", 1.20],
    "Trunk": ["#FC0301", 1.35],
}

river_attrs = {
    "Navigatable all year": ["#1b9e77", 1.5],
    "Seasonal Navigation": ["#d95f02", 1.5],
    "Navigatable to shallow vessels": ["#7570b3", 1.5],
    "Non Navigatable": ["#e7298a", 1.5],
}

land_cover_speed_map = {
    0: -9999.0,
    10: 2.5,
    11: 3,
    12: 3,
    20: 2,
    30: 3.24,
    40: 3.24,
    50: 1.62,
    60: 1.62,
    61: 1.62,
    62: 1.62,
    70: 3.24,
    71: 3.24,
    72: 3.24,
    80: 3.24,
    81: 3.24,
    82: 3.24,
    90: 3.24,
    100: 3,
    110: 3,
    120: 3,
    121: 3,
    122: 3,
    130: 4.86,
    140: 3,
    150: 3,
    151: 3,
    152: 3,
    153: 3,
    160: 2,
    170: 2,
    180: 2,
    190: 3,
    200: 3,
    201: 3,
    202: 3,
    210: 2,
    220: 1.62,
}


REF_PROJ_CONFIG = {
    "srs": (
        'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,\
            AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],\
            UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]GEOGCS["WGS 84",\
            DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],\
            AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],\
            AUTHORITY["EPSG","4326"]]GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,\
            298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],\
            UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]'
    ),
    "wkp": "EPSG:4326",
    "transform": Affine.from_gdal(
        24.150733709000065,
        0.008334173612994345,
        0,
        12.23635188000003,
        0,
        -0.008331170202664108,
    ),
    "ncolsrows": (1416, 1051),
}

travel_time_vars = [
    "landcover",
    "rivers",
    "roads",
    "national boundary",
    "dem",
    "slope",
    "railway",
    "inundation",
]
