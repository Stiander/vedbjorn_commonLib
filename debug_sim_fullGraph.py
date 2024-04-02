"""
    File : debug_sim_fullGraph.py
    Author : Stian Broen
    Date : 17.07.2022
    Description :

Contains functions to generate simulated (fake) market-situations.
This deploys a different data-model, which relies completely on Neo4J database, without using MongoDB

"""

# from standard Python
import random, string, datetime

# from common_library
from .defs import *
from .graph_funcs import *
from .location_funcs import *
from .db_insist import get_db

def randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))

def MAKE_FAKE_USER() :
    firstname = randomword(5)
    lastname = randomword(5)
    return {
        'email': firstname + '_' + lastname + '@fake.com',
        'phone' : '47' + str(random.randint(40000000, 49999999)) ,
        'name' : firstname[0].upper() + firstname[1:] + ' ' + lastname[0].upper() + lastname[1:] ,
        'firstname' : firstname[0].upper() + firstname[1:] ,
        'lastname' : lastname[0].upper() + lastname[1:] ,
        'fake' : True
    }

def MAKE_FAKE_BUY_REQUEST(user : dict, reserved_weeks : int) :
    return {
        'name'                : 'BUY_' + user['name'] ,
        'current_requirement' : 3 ,
        'reserved_weeks'      : reserved_weeks ,
        'reserve_target'      : '' , #< The specific SellRequest where there is a reservation
        'last_calced'         : 0 ,
        'claimed_by_driver'   : False ,
        'fake'                : True,
    }

def MAKE_FAKE_SELL_REQUEST(user : dict) :
    return {
        'name'               : 'SELL_' + user['name'],
        'current_capacity'   : 240 ,
        'amount_reserved'    : 0   ,
        'amount_staged'      : 0   ,
        'num_reserved'       : 0   ,
        'num_staged'         : 0   ,
        'prepare_for_pickup' : 0   ,
        'fake'               : True
    }

def MAKE_FAKE_DRIVE_REQUEST(user : dict) :
    return {
        'name'               : 'DRIVE_' + user['name'] ,
        'available'          : True ,
        'num_staged_pickups' : 0    ,
        'fake'               : True
    }

def generate_fake_users(num_bulks : int = -1 , num_users : int = 10, seed_pos : dict = {}, type : UserType = UserType.SELLER,
                         bulk_radius : float = 5000, local_spread_radius : float = 1000 , reserved_weeks : int = -1,
                        hard_users : list = None) -> list :

    if not hard_users :
        hard_users = list()

    users: list = []
    if seed_pos :
        bulk_center: dict = {
            'lat': seed_pos['lat'],
            'lng': seed_pos['lng']
        }
    else:
        bulk_center: dict = {
            'lat': EPICENTER['lat'] + random.uniform(-1, 1),
            'lng': EPICENTER['lng'] + random.uniform(-1, 1)
        }

    bulk_east = position_at_distance(bulk_center, bulk_radius, Direction.EAST)
    bulk_north = position_at_distance(bulk_center, bulk_radius, Direction.NORTH)
    dLat = abs(bulk_north['lat'] - bulk_center['lat'])
    dLng = abs(bulk_east['lng'] - bulk_center['lng'])

    if num_bulks > 0:
        for j in range(0, num_bulks):
            this_bulk_center: dict = {
                'lat': bulk_center['lat'] + random.uniform(-dLat, dLat),
                'lng': bulk_center['lng'] + random.uniform(-dLng, dLng)
            }

            spread_east = position_at_distance(this_bulk_center, local_spread_radius, Direction.EAST)
            spread_north = position_at_distance(this_bulk_center, local_spread_radius, Direction.NORTH)
            spread_dLat = abs(spread_east['lat'] - this_bulk_center['lat'])
            spread_dLng = abs(spread_north['lng'] - this_bulk_center['lng'])

            for i in range(0, num_users) :

                loc_attempts: int = 0
                loc = None
                location_name = ''
                while loc_attempts < 10:
                    loc_attempts = loc_attempts + 1
                    loc = location_info(
                        lat = this_bulk_center['lat'] + random.uniform(-spread_dLat, spread_dLat),
                        lng = this_bulk_center['lng'] + random.uniform(-spread_dLng, spread_dLng))
                    loc['fake'] = True
                    postcode = loc.get('postcode', '')
                    if not postcode:
                        continue
                    if not loc or not 'name' in loc or not 'municipality' in loc or not 'county' in loc:
                        continue
                    else:
                        try:
                            location_name = loc_to_graph(loc, {'fake': True}, {'fake': True})
                        except Exception as e:
                            continue
                        if location_name :
                            break

                if not loc or not location_name:
                    continue

                if len(users) < len(hard_users) :
                    user = hard_users[len(users)]
                else :
                    user = MAKE_FAKE_USER()

                user['location_name'] = location_name
                user_to_location(location_name, user , {'fake' : True})

                if type == UserType.BUYER:
                    buyrequest = MAKE_FAKE_BUY_REQUEST(user , reserved_weeks)
                    buyrequest_to_user(buyrequest, user, {'fake': True})
                elif type == UserType.SELLER:
                    sellrequest = MAKE_FAKE_SELL_REQUEST(user)
                    sellrequest_to_user(sellrequest, user, {'fake': True})
                elif type == UserType.DRIVER:
                    driverequest = MAKE_FAKE_DRIVE_REQUEST(user)
                    driverequest_to_user(driverequest, user, {'fake': True})

                user['pos'] = loc
                users.append(user)

    else:
        for i in range(0, num_users):

            loc_attempts : int = 0
            loc = None
            location_name = ''
            while loc_attempts < 10:
                loc_attempts = loc_attempts + 1
                loc = location_info(
                    lat=bulk_center['lat'] + random.uniform(-dLat, dLat),
                    lng=bulk_center['lng'] + random.uniform(-dLng, dLng))
                loc['fake'] = True
                postcode = loc.get('postcode', '')
                if not postcode :
                    continue
                if not loc or not 'name' in loc or not 'municipality' in loc or not 'county' in loc:
                    continue
                else:
                    try:
                        location_name = loc_to_graph(loc, {'fake': True}, {'fake': True})
                    except Exception as e:
                        continue
                    if location_name:
                        break

            if not loc or not location_name:
                continue

            if len(users) < len(hard_users):
                user = hard_users[len(users)]
            else:
                user = MAKE_FAKE_USER()

            user['location_name'] = location_name
            user_to_location(location_name, user , {'fake' : True})

            if type == UserType.BUYER:
                buyrequest = MAKE_FAKE_BUY_REQUEST(user , reserved_weeks)
                buyrequest_to_user(buyrequest, user , {'fake' : True})
            elif type == UserType.SELLER:
                sellrequest = MAKE_FAKE_SELL_REQUEST(user)
                sellrequest_to_user(sellrequest, user , {'fake' : True})
            elif type == UserType.DRIVER:
                driverequest = MAKE_FAKE_DRIVE_REQUEST(user)
                driverequest_to_user(driverequest, user , {'fake' : True})

            user['pos'] = loc
            users.append(user)

    return users

def simulate_horten_fullGraph() :
    print('----- SIMULATING HORTEN ---------')
    _started = datetime.datetime.utcnow().timestamp()
    sellers = generate_fake_users(
        num_bulks=0,
        num_users=2,
        seed_pos={'lat': 59.412497, 'lng': 10.454498},
        bulk_radius=3000,
        type=UserType.SELLER)

    for seller in sellers:
        generate_fake_users(
            num_bulks=0,
            num_users=4,
            seed_pos=seller['pos'],
            bulk_radius=1500,
            type=UserType.BUYER)

        generate_fake_users(
            num_bulks=0,
            num_users=1,
            seed_pos=seller['pos'],
            bulk_radius=1500,
            type=UserType.BUYER,
            reserved_weeks=4)

    # generate_fake_users(
    #     num_bulks=0,
    #     num_users=1,
    #     seed_pos=sellers[0]['pos'],
    #     bulk_radius=1500,
    #     type=UserType.DRIVER)

    print('----- DURATION : ', round(datetime.datetime.utcnow().timestamp() - _started, 1), ' seconds')
    print('----- DONE ---------')

def simulate_oslo_fullGraph() :
    print('----- SIMULATING OSLO ---------')
    _started = datetime.datetime.utcnow().timestamp()

    hard_sellers = [
        {'email': 'stian@broentech.no','phone' : '4745241592' , 'firstname' : 'Stian', 'lastname' : 'Broen SELLER' , 'name' : 'Stian Broen SELLER' , 'fake' : True}
    ]

    hard_drivers = [
        {'email': 'stian@vedbjorn.no', 'phone': '4745241593', 'firstname' : 'Stian', 'lastname' : 'Broen DRIVER', 'name': 'Stian Broen DRIVER', 'fake': True}
    ]
    hard_buyers = [
        {'email': 'stianbroen@gmail.com', 'phone': '4745241594', 'firstname' : 'Stian', 'lastname' : 'Broen BUYER', 'name': 'Stian Broen BUYER', 'fake': True}
    ]

    sellers = generate_fake_users(
        num_bulks=0,
        num_users=1,
        seed_pos={'lat': 59.908967, 'lng': 10.795021},
        bulk_radius=3000,
        type=UserType.SELLER,
        hard_users = hard_sellers
    )
    cnt = 0

    for seller in sellers:

        if cnt % 2 == 0:
            generate_fake_users(
                num_bulks=0,
                num_users=1,
                seed_pos=seller['pos'],
                bulk_radius=1000,
                type=UserType.DRIVER,
                hard_users=hard_drivers
            )
        cnt = cnt + 1

        generate_fake_users(
            num_bulks=0,
            num_users=1,
            seed_pos=seller['pos'],
            bulk_radius=1500,
            type=UserType.BUYER,
            reserved_weeks=3,
            hard_users = hard_buyers
        )

        generate_fake_users(
            num_bulks=0,
            num_users=1,
            seed_pos=seller['pos'],
            bulk_radius=1500,
            type=UserType.BUYER,
            reserved_weeks=0
        )

        if len(hard_drivers) > 0 :
            del hard_drivers[len(hard_drivers) - 1]

        if len(hard_buyers) > 0 :
            del hard_buyers[len(hard_buyers) - 1]

    print('----- DURATION : ', round(datetime.datetime.utcnow().timestamp() - _started, 1), ' seconds')
    print('----- DONE ---------')


def delete_simulation() :
    delete_and_detach('User', {'fake': True})
    delete_and_detach('SellRequest', {'fake': True})
    delete_and_detach('BuyRequest', {'fake': True})
    delete_and_detach('DriveRequest', {'fake': True})
    delete_and_detach('Location', {'fake': True})
    delete_and_detach('Geo', {'fake': True})

    delete_and_detach('User', {'fake': 'True'})
    delete_and_detach('SellRequest', {'fake': 'True'})
    delete_and_detach('BuyRequest', {'fake': 'True'})
    delete_and_detach('DriveRequest', {'fake': 'True'})
    delete_and_detach('Location', {'fake': 'True'})
    delete_and_detach('Geo', {'fake': 'True'})

    db = get_db()
    db.insist_on_delete_many('batchsell_requests', {})
    db.insist_on_delete_many('companies', {})
    db.insist_on_delete_many('notifications', {})
    db.insist_on_delete_many('files.chunks', {})
    db.insist_on_delete_many('files.files', {})
    db.insist_on_delete_many('matchloop_guide', {})
    db.insist_on_delete_many('returns', {})
    db.insist_on_delete_many('delivery_rejections', {})
    db.insist_on_delete_many('delivery_accept', {})
    db.insist_on_delete_many('deliveries', {'fake' : True})
    db.insist_on_delete_many('ongoing_routes', {'fake': True})
    db.insist_on_delete_many('pickups', {'fake': True})
    db.insist_on_delete_many('planned_routes', {'fake': True})
    db.insist_on_delete_many('vipps_internal_transfers', {'fake': True})
    db.insist_on_delete_many('vipps_payments_in', {'fake': True})
    db.insist_on_delete_many('vipps_payments_out', {'fake': True})
    db.insist_on_delete_many('wrapup_routes', {'fake': True})
