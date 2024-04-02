"""
    File : location_funcs.py
    Author : Stian Broen
    Date : 19.04.2022
    Description :

Contains functions related to location

"""

import time , os
from bson.objectid import ObjectId
from geopy.geocoders import Nominatim , GoogleV3
from haversine import Unit, inverse_haversine, Direction, haversine


GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY' , 'AIzaSyC7u-9hq27kASmDbVRvsc14jzsgqBsjp90')

def make_coordinate_name(lat : float, lng : float) :
    return str(round(lat, 6)) + ',' + str(round(lng, 6))

"""
    Function : coordinates_from_address

    Description :
        Return coordinates from address  (geolocation)

"""
def coordinates_from_address(road : str, postcode : int, municipality : str, county : str = '', country : str = '') :

    max_attempts = 10
    attempts = 0
    addr_str : str = road + ', ' + str(postcode) + ' ' + municipality
    if county :
        addr_str = addr_str + ', ' + county
    if country :
        addr_str = addr_str + ', ' + country

    while True:
        try:
            geolocator = GoogleV3(api_key=GOOGLE_API_KEY)
            location = geolocator.geocode(addr_str)
            if location :
                postcode : str = ''
                street_number : str = ''
                route : str = ''
                municipality : str = ''
                county : str = ''
                country : str = ''
                for component in location.raw['address_components'] :
                    type = component['types'][0]
                    if type == 'street_number' :
                        street_number = component['long_name']
                    elif type == 'route' :
                        route = component['long_name']
                    elif type == 'administrative_area_level_2' :
                        municipality = component['long_name'].replace(' kommune' , '').replace(' Municipality' , '')
                    elif type == 'administrative_area_level_1':
                        county = component['long_name']
                    elif type == 'country' :
                        country = component['long_name']
                    elif type == 'postal_code' :
                        postcode = component['long_name']

                return {
                    'lat' : location.latitude,
                    'lng' : location.longitude,
                    'place_id' : -1 ,
                    'osm_type' : '' ,
                    'osm_id' : -1 ,
                    'display_name' : location.address ,
                    'class' : location.raw['geometry']['location_type'] ,
                    'name' : make_coordinate_name(location.latitude, location.longitude) ,
                    'importance' : -1,
                    'road' : route + ' ' + street_number ,
                    'municipality' : municipality ,
                    'county' : county ,
                    'country' : country ,
                    'postcode' : postcode ,
                    'quarter' : '' ,
                    'village' : '' ,
                    'farm' : ''
                }
        except Exception as e:
            """
            Google failed. Try another service
            """
            try:
                geolocator = Nominatim(user_agent="Vedbjørn")
                location = geolocator.geocode(addr_str)
                if location:
                    raw = location.raw
                    return {
                        'lat': float(raw.get('lat', -1)),
                        'lng': float(raw.get('lon', -1)),
                        'place_id': raw.get('place_id', -1),
                        'osm_type': raw.get('osm_type', ''),
                        'osm_id': raw.get('osm_id', -1),
                        'display_name': raw.get('display_name', ''),
                        'class': raw.get('class', ''),
                        'type': raw.get('type', ''),
                        'importance': raw.get('importance', -1),
                        'name': make_coordinate_name(float(raw.get('lat', -1)), float(raw.get('lon', -1)))
                    }
                break
            except Exception as e :
                attempts = attempts + 1
                if attempts < max_attempts / 2 :
                    time.sleep(1)
                elif attempts < max_attempts:
                    time.sleep(5)
                else:
                    raise e
    return {}


"""
    Function : location_info

    Description :
        Return location information based on coordinates (reverse geolocation)

"""
def location_info(lat : float, lng : float) -> dict :
    max_attempts = 10
    attempts = 0
    while True :
        try:
            geolocator = GoogleV3(api_key=GOOGLE_API_KEY)
            location = geolocator.reverse(str(lat) + ', ' + str(lng))
            if location :
                postcode : str = ''
                street_number : str = ''
                route : str = ''
                municipality : str = ''
                county : str = ''
                country : str = ''
                for component in location.raw['address_components'] :
                    type = component['types'][0]
                    if type == 'street_number' :
                        street_number = component['long_name']
                    elif type == 'route' :
                        route = component['long_name']
                    elif type == 'administrative_area_level_2' :
                        municipality = component['long_name'].replace(' kommune' , '').replace(' Municipality' , '')
                    elif type == 'administrative_area_level_1':
                        county = component['long_name']
                    elif type == 'country' :
                        country = component['long_name']
                    elif type == 'postal_code' :
                        postcode = component['long_name']

                return {
                    'lat' : location.latitude,
                    'lng' : location.longitude,
                    'place_id' : -1 ,
                    'osm_type' : '' ,
                    'osm_id' : -1 ,
                    'display_name' : location.address ,
                    'class' : location.raw['geometry']['location_type'] ,
                    'name' : make_coordinate_name(location.latitude, location.longitude) ,
                    'importance' : -1,
                    'road' : route + ' ' + street_number ,
                    'municipality' : municipality ,
                    'county' : county ,
                    'country' : country ,
                    'postcode' : postcode ,
                    'quarter' : '' ,
                    'village' : '' ,
                    'farm' : ''
                }
            break
        except Exception as e:
            try:
                geolocator = Nominatim(user_agent="Vedbjørn")
                location = geolocator.reverse(str(lat) + ', ' + str(lng))
                if location:
                    raw = location.raw
                    address = raw.get('address', {})
                    return {
                        'lat': lat,
                        'lng': lng,
                        'place_id': raw.get('place_id', -1),
                        'osm_type': raw.get('osm_type', ''),
                        'display_name': raw.get('display_name', ''),
                        'road': address.get('road', ''),
                        'quarter': address.get('quarter', ''),
                        'village': address.get('village', ''),
                        'farm': address.get('farm', ''),
                        'municipality': address.get('municipality', ''),
                        'county': address.get('county', address.get('municipality' , '')),
                        'country': address.get('country', ''),
                        'postcode': str(address.get('postcode', '0')),
                        'name': make_coordinate_name(lat, lng)
                    }
                break
            except Exception as e:
                attempts = attempts + 1
                if attempts < max_attempts / 2 :
                    time.sleep(1)
                elif attempts < max_attempts:
                    time.sleep(5)
                else:
                    raise e

    return {}

"""
    Function : get_radian_diff_between_coordinates

    Description :

"""
def get_radian_diff_between_coordinates(pos_1 : dict , pos_2 : dict) :
    return pos_1['lat'] - pos_2['lat'] , pos_1['lng'] - pos_2['lng']

"""
    Function : distance_between_coordinates

    Description :

Return distance between 2 coordinates in meters

"""
def distance_between_coordinates(pos_1 : dict , pos_2 : dict) :
    return haversine((pos_2['lat'], pos_2['lng']),(pos_1['lat'], pos_1['lng']), unit=Unit.METERS)

"""
    Functions : position_at_distance
               position_at_distance_fromObj

    Description :

Retrieve coordinates for a position which is the argument distance away from the argument position, in the
argument direction

"""
def position_at_distance(pos : dict , meters : float, direction : Direction = Direction.EAST) :
    _pt = inverse_haversine((pos['lat'], pos['lng']), meters , direction, unit=Unit.METERS)
    return {'lat' : _pt[0] , 'lng' : _pt[1]}
def position_at_distance_fromObj(userPosObj : dict , meters : float, direction : Direction = Direction.EAST) :
    return position_at_distance(pos_from_userPosObj(userPosObj), meters, direction)

"""
    Function : userPosObj_is_usable

    Description :

Check that argument object is valid

"""
def userPosObj_is_usable(userPosObj : dict) :
    return (userPosObj != {} and userPosObj != None and
        'lat' in userPosObj and 'lng' in userPosObj and 'user' in userPosObj)

"""
    Function : pos_from_userPosObj

    Description :

"""
def pos_from_userPosObj(userPosObj : dict, db = None) :
    if 'lat' in userPosObj and 'lng' in userPosObj :
        return {'lat' : userPosObj['lat'] , 'lng' : userPosObj['lng']}
    elif 'loc' in userPosObj and 'lat' in userPosObj['loc'] and 'lng' in userPosObj['loc'] :
        return {'lat': userPosObj['loc']['lat'], 'lng': userPosObj['loc']['lng']}
    elif 'pos' in userPosObj and 'lat' in userPosObj['pos'] and 'lng' in userPosObj['pos'] :
        return {'lat': userPosObj['pos']['lat'], 'lng': userPosObj['pos']['lng']}
    else:
        if db and ('_id' in userPosObj or 'userId' in userPosObj):
            usid = ObjectId(userPosObj.get('_id' , userPosObj.get('userId' , '')))
            obj = db.insist_on_find_one_q('driverequests' , {'userId' : usid})
            if not obj :
                obj = db.insist_on_find_one_q('sellrequests', {'userId': usid})
                if not obj :
                    obj = db.insist_on_find_one_q('buyrequests', {'userId': usid})
                    if not obj :
                        raise Exception('userPosObj does not have position : ', userPosObj)
            return pos_from_userPosObj(obj)
        raise Exception('userPosObj does not have position : ' , userPosObj)

"""
    Function : sorting functions for list.sort

    Description :

Just a convenience for list that needs to sort

"""
def sort_by_distance(e) :
    return e.get('distance', 0)
def sort_by_combined_competitor_distance(e) :
    return e.get('combined_competitor_distance', 0)
def sort_by_my_distance(e) :
    return e.get('my_distance', 0)