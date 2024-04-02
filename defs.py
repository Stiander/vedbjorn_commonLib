"""
    File : defs.py
    Author : Stian Broen
    Date : 13.04.2022
    Description :

Contains definitions needed for the matching algorithm

"""

from enum import Enum

DEG_DIST_LAT : float = 1.0
DEG_DIST_LNG : float = 1.0
INITIAL_LOCAL_MARKET_RADIUS = 15000 # meters

ONE_DAY    = 60 * 60 * 24
TWO_DAYS   = ONE_DAY * 2
THREE_DAYS = ONE_DAY * 3
FOUR_DAYS  = ONE_DAY * 4
FIVE_DAYS  = ONE_DAY * 5
SIX_DAYS   = ONE_DAY * 6
SEVEN_DAYS = ONE_DAY * 7
EIGHT_DAYS = ONE_DAY * 8
NINE_DAYS  = ONE_DAY * 9
TEN_DAYS   = ONE_DAY * 10

MAX_ALLOWED_RESERVATION_WEEKS = 4
MAX_ALLOWED_REQUEST_ITEMS = 8
IS_FAKE = True

PRICE_PER_WEEK_RESERVATION_PER_BAG = 20

class UserType(Enum):
    SELLER = 1
    BUYER = 2
    DRIVER = 4

class GeoTypes(Enum):
    LOCATION = 0
    ROAD = 1
    POSTCODE = 2
    MUNICIPALITY = 4
    COUNTY = 8
    COUNTRY = 16

EPICENTER : dict = {
    'lat' : 61.295476,
    'lng' : 8.890690
}

ITERATION_SLEEP_TIME_S = 3600