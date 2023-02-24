import geocoder
import geohash2


def get_lat(address, city, country):
    location = f"{address}, {city}, {country}"
    g = geocoder.arcgis(location)
    if g.ok:
        return g.lat
    else:
        return None

def get_lon(address, city, country):
    location = f"{address}, {city}, {country}"
    g = geocoder.arcgis(location)
    if g.ok:
        return g.lng
    else:
        return None

def get_geohash(lat, long, precision=4):
    return geohash2.encode(lat, long, precision=precision)