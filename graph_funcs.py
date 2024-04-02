"""
    File : graph_funcs.py
    Author : Stian Broen
    Date : 19.04.2022
    Description :

Contains functions related to Neo4J

"""
import datetime

from .db_graph import GraphDB as GraphDB , to_relaxed_json_str
from .db_insist import all_objectids_to_str

neo = None

def merge_dicts(dict1 : dict , dict2 : dict) -> dict:
    """

    :param dict1:
    :param dict2:
    :return:
    """
    if dict1 != {} and dict2 == {} :
        return dict1
    if dict1 == {} and dict2 != {} :
        return dict2
    if dict1 == {} and dict2 == {} :
        return {}
    return {k: v for d in [dict1, dict2] for k, v in d.items()}

def get_neo() :
    """

    :return:
    """
    global neo
    if neo == None:
        neo = GraphDB(verbose=False)
    return neo


def set_reserved_weeks_BuyRequest(buyRequest_name : str, reserved_weeks : int) :
    """

    :param buyRequest_name:
    :param reserved_weeks:
    :return:
    """
    query = 'MATCH (s:BuyRequest) WHERE (s.name = $name) ' \
            'SET s.reserved_weeks = $reserved_weeks ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], name=buyRequest_name, reserved_weeks=reserved_weeks)

def set_current_requirement_BuyRequest(buyRequest_name : str, current_requirement : int) :
    """

    :param buyRequest_name:
    :param current_requirement:
    :return:
    """
    query = 'MATCH (s:BuyRequest) WHERE (s.name = $name) ' \
            'SET s.current_requirement = $current_requirement ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], name=buyRequest_name, current_requirement=current_requirement)

def update_stock_sellRequest(sellRequest_name : str , new_capacity : int , new_amount_reserved : int ,
                             new_amount_staged : int, new_num_reserved : int, new_num_staged : int ) :
    """

    :param sellRequest_name:
    :param new_capacity:
    :param new_amount_reserved:
    :param new_amount_staged:
    :param new_num_reserved:
    :param new_num_staged:
    :return:
    """
    query = 'MATCH (s:SellRequest) WHERE (s.name = $name) ' \
                  'SET s.current_capacity = $new_capacity ' \
                  'SET s.amount_reserved = $new_amount_reserved ' \
                  'SET s.amount_staged = $new_amount_staged ' \
                  'SET s.num_staged = $new_num_staged ' \
                  'SET s.num_reserved = $new_num_reserved ' \
                  'RETURN s'
    return get_neo().run_query(query, ['s'],
                             name=sellRequest_name,
                             new_capacity=new_capacity,
                             new_amount_reserved=new_amount_reserved,
                             new_amount_staged=new_amount_staged,
                             new_num_staged=new_num_staged,
                             new_num_reserved=new_num_reserved)


def update_current_capacity_sellRequest(sellRequest_name : str , new_capacity : int) :
    """

    :param sellRequest_name:
    :param new_capacity:
    :return:
    """
    query = 'MATCH (s:SellRequest) WHERE (s.name = $sellRequest_name) ' \
            'SET s.current_capacity = $new_capacity ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], sellRequest_name=sellRequest_name, new_capacity=new_capacity)

def update_available_driveRequest(driveRequest_name : str , available : bool) :
    """

    :param driveRequest_name:
    :param available:
    :return:
    """
    query = 'MATCH (s:DriveRequest) WHERE (s.name = $driveRequest_name) ' \
            'SET s.available = $available ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], driveRequest_name=driveRequest_name, available=available)

def increment_prepare_for_pickup_for_SellRequest(name : str , prepare_increment_by : int) :
    """

    :param name:
    :param prepare_increment_by:
    :return:
    """
    query = 'MATCH (s:SellRequest) WHERE (s.name = $name) ' \
            'SET s.prepare_for_pickup = s.prepare_for_pickup + $prepare_increment_by ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], name=name, prepare_increment_by=prepare_increment_by)

def decrement_prepare_for_pickup_for_SellRequest(name : str , decrement_increment_by : int) :
    """

    :param name:
    :param decrement_increment_by:
    :return:
    """
    query = 'MATCH (s:SellRequest) WHERE (s.name = $name) ' \
            'SET s.prepare_for_pickup = s.prepare_for_pickup - $decrement_increment_by ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], name=name, decrement_increment_by=decrement_increment_by)

def set_claimed_by_driver_on_buyRequest(name : str, claimed : bool) :
    """

    :param name:
    :param claimed:
    :return:
    """
    query = 'MATCH (b:BuyRequest) WHERE (b.name = $name) ' \
            'SET b.claimed_by_driver = $claimed ' \
            'RETURN b'
    return get_neo().run_query(query, ['b'], name=name, claimed=claimed)

def set_last_calced_BuyRequest(buyRequest_name : str, next_last_calced_timestamp : float) :
    """

    :param buyRequest_name:
    :param next_last_calced_timestamp:
    :return:
    """
    query = 'MATCH (s:BuyRequest) WHERE (s.name = $buyRequest_name) ' \
            'SET s.last_calced = $next_last_calced_timestamp ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], buyRequest_name=buyRequest_name, next_last_calced_timestamp=next_last_calced_timestamp)

def remove_reservation(buyRequest_name : str) :
    """

    :param buyRequest_name:
    :return:
    """
    query = 'MATCH () -[r:RESERVATION]-> () ' \
            'WHERE r.BuyRequest_name = $buyRequest_name ' \
            'DELETE r'
    return get_neo().run_query(query, [], buyRequest_name=buyRequest_name)

def remove_staged_sell(buyRequest_name : str) :
    """

    :param buyRequest_name:
    :return:
    """
    query = 'MATCH () -[r:STAGED_SELL]-> () ' \
            'WHERE r.BuyRequest_name = $buyRequest_name ' \
            'DELETE r'
    return get_neo().run_query(query, [], buyRequest_name=buyRequest_name)

def remove_travel_to_pickup(driverRequestName : str) :
    """

    :param driverRequestName:
    :return:
    """
    query = 'MATCH () -[ttp:TRAVEL_TO_PICKUP]-> () ' \
            'WHERE ttp.driveRequest = $driverRequestName ' \
            'DELETE ttp'
    return get_neo().run_query(query, [], driverRequestName=driverRequestName)

def remove_staged_driver(driverRequestName : str) :
    """

    :param driverRequestName:
    :return:
    """
    query = 'MATCH () -[sd:STAGED_DRIVER]-> (dr:DriveRequest) ' \
            'WHERE sd.DriveRequest_name = $driverRequestName ' \
            'SET dr.num_staged_pickups = 0 ' \
            'DELETE sd'
    return get_neo().run_query(query, [], driverRequestName=driverRequestName)

def remove_travel_to_deliver(driverRequestName : str) :
    """

    :param driverRequestName:
    :return:
    """
    query = 'MATCH () -[ttp:TRAVEL_TO_DELIVER]-> () ' \
            'WHERE ttp.driveRequest = $driverRequestName ' \
            'DELETE ttp'
    return get_neo().run_query(query, [], driverRequestName=driverRequestName)

def remove_buyRequest(buyRequest_name : str) :
    """

    :param buyRequest_name:
    :return:
    """
    query = 'MATCH (s:BuyRequest) WHERE (s.name = $buyRequest_name) ' \
            'DETACH DELETE s'
    return get_neo().run_query(query, [], buyRequest_name=buyRequest_name)

def remove_sellRequest(sellRequest_name : str) :
    """

    :param sellRequest_name:
    :return:
    """
    query = 'MATCH (s:SellRequest) WHERE (s.name = $sellRequest_name) ' \
            'DETACH DELETE s'
    return get_neo().run_query(query, [], sellRequest_name=sellRequest_name)

def remove_driveRequest(driveRequest_name : str) :
    """

    :param driveRequest_name:
    :return:
    """
    query = 'MATCH (s:DriveRequest) WHERE (s.name = $driveRequest_name) ' \
            'DETACH DELETE s'
    return get_neo().run_query(query, [], driveRequest_name=driveRequest_name)

def set_SellRequest_for_BuyRequest_reservation(name : str , sellRequest_name : str) :
    """

    :param name:
    :param sellRequest_name:
    :return:
    """
    query = 'MATCH (s:BuyRequest) WHERE (s.name = $name) ' \
            'SET s.reserve_target = $sellRequest_name ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], name=name, sellRequest_name=sellRequest_name)

def update_num_reserved_for_SellRequest(name : str , num_reserved : int) :
    """

    :param name:
    :param num_reserved:
    :return:
    """
    query = 'MATCH (s:SellRequest) WHERE (s.name = $name) ' \
            'SET s.num_reserved = $num_reserved ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], name=name, num_reserved=num_reserved)

def update_num_staged_for_SellRequest(name : str , num_staged : int) :
    """

    :param name:
    :param num_staged:
    :return:
    """
    query = 'MATCH (s:SellRequest) WHERE (s.name = $name) ' \
            'SET s.num_staged = $num_staged ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], name=name, num_staged=num_staged)

def update_num_staged_pickups_for_DriveRequest(name : str , num_staged_pickups : int) :
    """

    :param name:
    :param num_staged_pickups:
    :return:
    """
    query = 'MATCH (s:DriveRequest) WHERE (s.name = $name) ' \
            'SET s.num_staged_pickups = $num_staged_pickups ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], name=name , num_staged_pickups=num_staged_pickups)

def update_amount_reserved_for_SellRequest(name : str , amount_reserved : int) :
    """

    :param name:
    :param amount_reserved:
    :return:
    """
    query = 'MATCH (s:SellRequest) WHERE (s.name = $name) ' \
            'SET s.amount_reserved = $amount_reserved ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], name=name , amount_reserved=amount_reserved)

def update_amount_staged_for_SellRequest(name : str , amount_staged : int) :
    """

    :param name:
    :param amount_staged:
    :return:
    """
    query = 'MATCH (s:SellRequest) WHERE (s.name = $name) ' \
            'SET s.amount_staged = $amount_staged ' \
            'RETURN s'
    return get_neo().run_query(query, ['s'], name=name , amount_staged=amount_staged)

def remove_all_reservations() :
    """

    :return:
    """
    query = 'MATCH (s:SellRequest) <-[r:RESERVATION]- (:BuyRequest) ' \
            'SET s.num_reserved = 0 ' \
            'SET s.amount_reserved = 0 ' \
            'DELETE r'
    return get_neo().run_query(query, [''])

def remove_all_logistics() :
    """

    :return:
    """
    query = 'MATCH (s:SellRequest) -[sd:STAGED_DRIVER]-> (d:DriveRequest) ' \
            'SET d.num_staged_pickups = 0 ' \
            'DELETE sd'
    return get_neo().run_query(query, [''])

def remove_all_travel_to_pickups() :
    """

    :return:
    """
    query = 'MATCH (:Location) -[ttp:TRAVEL_TO_PICKUP]-> (:Location) ' \
            'DELETE ttp'
    return get_neo().run_query(query, [''])

def remove_all_travel_to_delivers() :
    """

    :return:
    """
    query = 'MATCH (:Location) -[ttd:TRAVEL_TO_DELIVER]-> (:Location) ' \
            'DELETE ttd'
    return get_neo().run_query(query, [''])

def remove_all_travels() :
    remove_all_travel_to_pickups()
    remove_all_travel_to_delivers()

def remove_all_staged_sells() :
    """

    :return:
    """
    query = 'MATCH (s:SellRequest) <-[ss:STAGED_SELL]- (d:BuyRequest) ' \
            'SET s.num_staged = 0 ' \
            'SET s.amount_staged = 0 ' \
            'DELETE ss'
    return get_neo().run_query(query, [''])

def mark_pickup_relationship(driveRequest_name : str, sellRequest_name : str, key : str, value) :
    """

    :param driveRequest_name:
    :param sellRequest_name:
    :param key:
    :param value:
    :return:
    """
    query = 'MATCH () <-[rel:TRAVEL_TO_PICKUP]- () ' \
            'WHERE ( rel.driveRequest = $driveRequest_name AND rel.sellRequest_name = $sellRequest_name ) ' \
            'SET rel.' + key + ' = $value ' \
            'RETURN rel'
    return get_neo().run_query(query, ['rel'],
                               driveRequest_name=driveRequest_name,
                               sellRequest_name=sellRequest_name,
                               value=value)

def mark_delivery_relationship(driveRequest_name : str, buyRequest_name : str, key : str, value) :
    """

    :param driveRequest_name:
    :param buyRequest_name:
    :param key:
    :param value:
    :return:
    """
    query = 'MATCH () <-[rel:TRAVEL_TO_DELIVER]- () ' \
            'WHERE ( rel.driveRequest = $driveRequest_name AND rel.buyRequest_name = $buyRequest_name ) ' \
            'SET rel.' + key + ' = $value ' \
            'RETURN rel'
    return get_neo().run_query(query, ['rel'],
                               driveRequest_name=driveRequest_name,
                               buyRequest_name=buyRequest_name,
                               value=value)

def insert_travel_from_to(travel_from : dict , travel_to : dict , travel_name : str , relationship_meta : dict) :
    neo = get_neo()
    cypher_queries: list = []
    cypher_queries.append(neo.connect_nodes_makeCypherQuery(
        sourceNodeNames=['Location'],
        sourceNodeMeta=all_objectids_to_str(travel_from),
        sourceVariableName='from',
        relationshipName=travel_name,
        relationShipMeta=all_objectids_to_str(relationship_meta),
        relationshipVariableName='R',
        targetNodeNames=['Location'],
        targetNodeMeta=all_objectids_to_str(travel_to),
        targetVariableName='to'))

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    neo.write_multi(cypher_queries)

def insert_reservation(buyReq : dict , sellReq : dict , relationship_meta : dict = {}) :
    neo = get_neo()
    cypher_queries: list = []
    cypher_queries.append(neo.connect_nodes_makeCypherQuery(
        sourceNodeNames=['BuyRequest'],
        sourceNodeMeta=all_objectids_to_str(buyReq),
        sourceVariableName='BR',
        relationshipName='RESERVATION',
        relationShipMeta=all_objectids_to_str(relationship_meta),
        relationshipVariableName='R',
        targetNodeNames=['SellRequest'],
        targetNodeMeta=all_objectids_to_str(sellReq),
        targetVariableName='S'))

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    neo.write_multi(cypher_queries)

def insert_stagesell(buyReq : dict , sellReq : dict , relationship_meta : dict = {}) :
    neo = get_neo()
    cypher_queries: list = []
    cypher_queries.append(neo.connect_nodes_makeCypherQuery(
        sourceNodeNames=['BuyRequest'],
        sourceNodeMeta=all_objectids_to_str(buyReq),
        sourceVariableName='BR',
        relationshipName='STAGED_SELL',
        relationShipMeta=all_objectids_to_str(relationship_meta),
        relationshipVariableName='R',
        targetNodeNames=['SellRequest'],
        targetNodeMeta=all_objectids_to_str(sellReq),
        targetVariableName='S'))

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    neo.write_multi(cypher_queries)

def insert_stagedrive(driveReq : dict , sellReq : dict , relationship_meta : dict = {}) :
    neo = get_neo()
    cypher_queries: list = []
    cypher_queries.append(neo.connect_nodes_makeCypherQuery(
        sourceNodeNames=['SellRequest'],
        sourceNodeMeta=all_objectids_to_str(sellReq),
        sourceVariableName='SR',
        relationshipName='STAGED_DRIVER',
        relationShipMeta=all_objectids_to_str(relationship_meta),
        relationshipVariableName='R',
        targetNodeNames=['DriveRequest'],
        targetNodeMeta=all_objectids_to_str(driveReq),
        targetVariableName='D'))

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    neo.write_multi(cypher_queries)

def get_pickup_from_driver_home(driver_home_name : str) :
    """

    :param driver_home_name:
    :return:
    """
    query = 'MATCH (home:Location) -[ret:TRAVEL_TO_PICKUP]-> (pickup:Location) '\
            'WHERE (home.name = $driver_home_name) '\
            'RETURN ret'
    return get_neo().run_query(query, ['ret'], driver_home_name=driver_home_name)

def get_reservation(name : str) :
    """

    :param name:
    :return:
    """
    query = 'MATCH (s:SellRequest) <-[r:RESERVATION]- (b:BuyRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) '\
            'WHERE (s.name = $name) '\
            'RETURN b , u , l , r , s'
    return get_neo().run_query(query, ['b', 'u', 'l', 'r', 's'], name=name)

def get_reservations_in_county(county : str) :
    """

    :param county:
    :return:
    """
    query = 'MATCH (s:SellRequest) <-[r:RESERVATION]- (b:BuyRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) '\
            'WHERE (b.reserved_weeks > 0 AND l.county = $county) '\
            'RETURN b , u , l , r , s'
    return get_neo().run_query(query, ['b', 'u', 'l', 'r', 's'], county=county)

def get_staged_sells_in_county(county : str) :
    """

    :param county:
    :return:
    """
    query = 'MATCH (s:SellRequest) <-[r:STAGED_SELL]- (b:BuyRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) '\
            'WHERE (b.reserved_weeks <= 0 AND l.county = $county) '\
            'RETURN b , u , l , r , s'
    return get_neo().run_query(query, ['b', 'u', 'l' , 'r' , 's'], county=county)

def get_staged_sells_for_sellreq(name : str) :
    """

    :param name:
    :return:
    """
    query = 'MATCH (s:SellRequest) <-[r:STAGED_SELL]- (b:BuyRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) '\
            'WHERE (s.name = $name) '\
            'RETURN b , u , l'
    return get_neo().run_query(query, ['b', 'u', 'l'], name=name)

def get_reservations_for_sellreq(name : str) :
    """

    :param name:
    :return:
    """
    query = 'MATCH (s:SellRequest) <-[r:RESERVATION]- (b:BuyRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) '\
            'WHERE (s.name = $name) '\
            'RETURN b , u , l'
    return get_neo().run_query(query, ['b', 'u', 'l'], name=name)

def get_staged_drives_in_county(county : str) :
    """

    :param county:
    :return:
    """
    query = 'MATCH (s:SellRequest) -[sd:STAGED_DRIVER]-> (dr:DriveRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) '\
            'WHERE (l.county = $county) '\
            'RETURN dr , u , l , s , sd'
    return get_neo().run_query(query, ['dr' , 'u' , 'l' , 's' , 'sd'], county=county)

def get_user_location(email : str) :
    """

    :param email:
    :return:
    """
    query = 'MATCH (u:User) -[:IS_HOME]-> (l:Location) '\
            'WHERE (u.email = $email) '\
            'RETURN l'
    return get_neo().run_query(query, ['l'], email=email)

def get_driver_location(name : str) :
    """

    :param name:
    :return:
    """
    query = 'MATCH (dr:DriveRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) '\
            'WHERE (dr.name = $name) '\
            'RETURN l'
    return get_neo().run_query(query, ['l'], name=name)

def get_staged_drives_in_county_both_locations(county : str) :
    """

    :param county:
    :return:
    """
    query = 'MATCH (l1:Location) <-[:IS_HOME]- (u1:User) -[:WANTS_TO]-> (s:SellRequest) -[sd:STAGED_DRIVER]-> (dr:DriveRequest) <-[:WANTS_TO]- (u2:User) -[:IS_HOME]-> (l2:Location) '\
            'WHERE (l1.county = $county) '\
            'RETURN l1 , u1 , s , dr , u2, l2'
    return get_neo().run_query(query, ['l1', 'u1', 's', 'dr', 'u2', 'l2'], county=county)

def get_staged_drives_in_county_both_locations_multi(county : str) :
    """

    :param county:
    :return:
    """
    query = 'MATCH (l:Location) <-[:IS_HOME]- (u:User) -[:WANTS_TO]-> (s:SellRequest) -[sd:STAGED_DRIVER]-> (dr:DriveRequest) <-[:WANTS_TO]- (u:User)' \
            'WHERE (l.county = $county) ' \
            'RETURN l , u , s , dr'
    return get_neo().run_query(query, ['l','u','s', 'dr'], county=county)

def get_drivers_in_county(county : str , available : bool = True) :
    """

    :param county:
    :param available:
    :return:
    """
    query = 'MATCH (d:DriveRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE (l.county = $county AND d.available = ' + str(available) + ' ) ' \
            'RETURN d , u , l'

    return get_neo().run_query(query, ['d','u','l'], county=county)

def get_all_drivers_in_county(county : str) :
    """
    :param county:
    :return:
    """
    query = 'MATCH (d:DriveRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE (l.county = $county) ' \
            'RETURN d , u , l'
    return get_neo().run_query(query, ['d','u','l'], county=county)

def get_all_sellers_in_county(county : str) :
    """
    :param county:
    :return:
    """
    query = 'MATCH (d:SellRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE (l.county = $county) ' \
            'RETURN d , u , l'
    return get_neo().run_query(query, ['d','u','l'], county=county)

def get_all_buyers_in_county(county : str) :
    """
    :param county:
    :return:
    """
    query = 'MATCH (d:BuyRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE (l.county = $county) ' \
            'RETURN d , u , l'
    return get_neo().run_query(query, ['d','u','l'], county=county)

def set_driver_available(name : str, available : bool) :
    """

    :param name:
    :param available:
    :return:
    """
    query = 'MATCH (d:DriveRequest) ' \
            'WHERE (d.name = $name) ' \
            'SET d.available = ' + str(available)
    return get_neo().run_query(query, [], name=name)

def set_driver_available_again_time(name : str, timestamp : float) :
    """

    :param name:
    :param timestamp:
    :return:
    """
    query = 'MATCH (d:DriveRequest) ' \
            'WHERE (d.name = $name) ' \
            'SET d.available_again_time = ' + str(timestamp)
    return get_neo().run_query(query, [], name=name)

def get_sell_requests_in_county(county : str) :
    """

    :param county:
    :return:
    """
    query = 'MATCH (s:SellRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE (l.county = $county) ' \
            'RETURN s , u , l'
    return get_neo().run_query(query, ['s', 'u', 'l'], county=county)

def get_buy_requests_in_county(county : str) :
    """

    :param county:
    :return:
    """
    query = 'MATCH (s:BuyRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE (l.county = $county) ' \
            'RETURN s , u , l'
    return get_neo().run_query(query, ['s', 'u', 'l'], county=county)

def get_drive_requests_in_county(county : str) :
    """

    :param county:
    :return:
    """
    query = 'MATCH (s:DriveRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE (l.county = $county) ' \
            'RETURN s , u , l'
    return get_neo().run_query(query, ['s', 'u', 'l'], county=county)

def get_sell_requests_in_muni(county : str, municipality : str) :
    """

    :param county:
    :param municipality:
    :return:
    """
    query = 'MATCH (s:SellRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE ((l.county = $county) AND ' \
                '(l.municipality = $municipality) ) ' \
            'RETURN s , u , l'
    return get_neo().run_query(query, ['s', 'u', 'l'], county=county, municipality=municipality)

def get_buy_requests_in_muni(county : str, municipality : str) :
    """

    :param county:
    :param municipality:
    :return:
    """
    query = 'MATCH (s:BuyRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE ((l.county = $county) AND ' \
                '(l.municipality = $municipality) ) ' \
            'RETURN s , u , l'
    return get_neo().run_query(query, ['s', 'u', 'l'], county=county, municipality=municipality)

def get_drive_requests_in_muni(county : str, municipality : str) :
    """

    :param county:
    :param municipality:
    :return:
    """
    query = 'MATCH (s:DriveRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE ((l.county = $county) AND ' \
                '(l.municipality = $municipality) ) ' \
            'RETURN s , u , l'
    return get_neo().run_query(query, ['s', 'u', 'l'], county=county, municipality=municipality)

def get_buyrequests_with_reservations_in_county(county : str , minimum_age : float ,
    calc_time : datetime = datetime.datetime.utcnow() , claimed_by_driver : bool = False) :
    """

    :param county:
    :param minimum_age:
    :param calc_time:
    :param claimed_by_driver:
    :return:
    """

    query = 'MATCH (b:BuyRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) '\
            'WHERE ((b.reserved_weeks) > 0 AND ' \
                '(l.county = $county) AND ' \
                '(b.claimed_by_driver = $claimed_by_driver) AND ' + \
                '(' + str(minimum_age) + ' < (' + str(calc_time.timestamp()) + ' - b.last_calced) ) ) ' \
            'RETURN b , u , l'
    return get_neo().run_query(query, ['b', 'u', 'l'], county=county, claimed_by_driver=claimed_by_driver)

def get_buyrequests_without_reservations_in_county(county : str , minimum_age : float ,
    calc_time : datetime = datetime.datetime.utcnow() , claimed_by_driver : bool = False) :
    """

    :param county:
    :param minimum_age:
    :param calc_time:
    :param claimed_by_driver:
    :return:
    """

    query = 'MATCH (b:BuyRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) '\
            'WHERE ((b.reserved_weeks) <= 0 AND ' \
               '(l.county = $county) AND ' \
               '(b.claimed_by_driver = $claimed_by_driver) AND ' + \
               '(' + str(minimum_age) + ' < (' + str(calc_time.timestamp()) + ' - b.last_calced) ) ) ' \
            'RETURN b , u , l'
    return get_neo().run_query(query, ['b', 'u', 'l'], county=county, claimed_by_driver=claimed_by_driver)

def get_sellers_in_postcode_capacity(postcode : str) :
    """

    :param postcode:
    :return:
    """
    query = 'MATCH (s:SellRequest) <-[:WANTS_TO]- (u:User) -[:IS_HOME]-> (l:Location) ' \
            'WHERE (l.postcode = $postcode AND (s.current_capacity - s.amount_reserved - s.amount_staged) >= 0) ' + \
            'RETURN s , u , l'
    return get_neo().run_query(query, ['s' , 'u' , 'l'], postcode=postcode)

def get_postcodes_in_county(name : str) :
    """

    :param name:
    :return:
    """
    query = 'MATCH (p:Postcode) -[:IS_LOCATED_IN]-> ' \
               '(:Municipality) -[:IS_LOCATED_IN]-> ' \
               '(c:County) ' \
            'WHERE (c.name = $name) ' + \
            'RETURN p'
    return get_neo().run_query(query, ['p'], name=name)

def get_location_with_name(name : str) :
    """

    :param name:
    :return:
    """
    query = 'MATCH (l:Location) ' \
            'WHERE (l.name = $name) ' + \
            'RETURN l'
    return get_neo().run_query(query, ['l'], name=name)

def get_user_with_phone(phone : str) :
    """

    :param phone:
    :return:
    """
    query = 'MATCH (u:User) ' \
            'WHERE (u.phone = $phone) ' + \
            'RETURN u'
    return get_neo().run_query(query, ['u'], phone=phone)

def get_user_with_email(email : str) :
    """

    :param email:
    :return:
    """
    query = 'MATCH (u:User) ' \
            'WHERE (u.email = $email) ' + \
            'RETURN u'
    return get_neo().run_query(query, ['u'], email=email)

def get_buyrequests_with_email(email : str) :
    """

    :param email:
    :return:
    """
    query = 'MATCH (b:BuyRequest) <-[:WANTS_TO]- (u:User) '\
            'WHERE (u.email = $email) ' \
            'RETURN b'
    return get_neo().run_query(query, ['b'], email=email)

def get_buyrequests() :
    """

    :param email:
    :return:
    """
    query = 'MATCH (b:BuyRequest) <-[:WANTS_TO]- (u:User) '\
            'RETURN b , u'
    return get_neo().run_query(query, ['b' , 'u'])


def get_sellrequests_with_email(email : str) :
    """

    :param email:
    :return:
    """
    query = 'MATCH (b:SellRequest) <-[:WANTS_TO]- (u:User) '\
            'WHERE (u.email = $email) ' \
            'RETURN b'
    return get_neo().run_query(query, ['b'], email=email)


def get_driverequests_with_email(email : str) :
    """

    :param email:
    :return:
    """
    query = 'MATCH (b:DriveRequest) <-[:WANTS_TO]- (u:User) '\
            'WHERE (u.email = $email) ' \
            'RETURN b'
    return get_neo().run_query(query, ['b'], email=email)

def get_buyrequests_with_phone(phone : str) :
    """

    :param phone:
    :return:
    """
    query = 'MATCH (b:BuyRequest) <-[:WANTS_TO]- (u:User) '\
            'WHERE (u.phone = $phone) ' \
            'RETURN b'
    return get_neo().run_query(query, ['b'], phone=phone)

def get_sellrequests_with_phone(phone : str) :
    """

    :param phone:
    :return:
    """
    query = 'MATCH (b:SellRequest) <-[:WANTS_TO]- (u:User) '\
            'WHERE (u.phone = $phone) ' \
            'RETURN b'
    return get_neo().run_query(query, ['b'], phone=phone)


def get_driverequests_with_phone(phone : str) :
    """

    :param phone:
    :return:
    """
    query = 'MATCH (b:DriveRequest) <-[:WANTS_TO]- (u:User) '\
            'WHERE (u.phone = $phone) ' \
            'RETURN b'
    return get_neo().run_query(query, ['b'], phone=phone)

def get_user_with_driverequest_name(name : str) :
    """

    :param name:
    :return:
    """
    query = 'MATCH (d:DriveRequest) <-[:WANTS_TO]- (u:User) '\
            'WHERE (d.name = $name) ' \
            'RETURN u'
    return get_neo().run_query(query, ['u'], name=name)

def get_user_with_buyrequest_name(name : str) :
    """

    :param name:
    :return:
    """
    query = 'MATCH (d:BuyRequest) <-[:WANTS_TO]- (u:User) '\
            'WHERE (d.name = $name) ' \
            'RETURN u'
    return get_neo().run_query(query, ['u'], name=name)

def get_user_with_sellrequest_name(name : str) :
    """

    :param name:
    :return:
    """
    query = 'MATCH (d:SellRequest) <-[:WANTS_TO]- (u:User) '\
            'WHERE (d.name = $name) ' \
            'RETURN u'
    return get_neo().run_query(query, ['u'], name=name)

def user_to_location(location_name : str, user : dict , relationship_meta : dict = None) :
    if relationship_meta == None :
        relationship_meta = dict()
    neo = get_neo()
    cypher_queries: list = []
    cypher_queries.append(neo.connect_nodes_makeCypherQuery(
        sourceNodeNames          = ['User'],
        sourceNodeMeta           = all_objectids_to_str(user),
        sourceVariableName       = 'NEW_USER',
        relationshipName         = 'IS_HOME',
        relationShipMeta         = all_objectids_to_str(relationship_meta),
        relationshipVariableName = 'TO_LOCATION',
        targetNodeNames          = ['Location'],
        targetNodeMeta           = {'name': location_name},
        targetVariableName       = 'LOCATION'))

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    neo.write_multi(cypher_queries)

def buyrequest_to_user(buyrequest : dict, user : dict , relationship_meta : dict = None) :
    if relationship_meta == None :
        relationship_meta = dict()
    neo = get_neo()
    cypher_queries: list = []
    cypher_queries.append(neo.connect_nodes_makeCypherQuery(
        sourceNodeNames          = ['User'],
        sourceNodeMeta           = all_objectids_to_str(user),
        sourceVariableName       = 'U1',
        relationshipName         = 'WANTS_TO',
        relationShipMeta         = all_objectids_to_str(relationship_meta),
        relationshipVariableName = 'R1',
        targetNodeNames          = ['BuyRequest'],
        targetNodeMeta           = all_objectids_to_str(buyrequest),
        targetVariableName       = 'B1'))

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    neo.write_multi(cypher_queries)

def sellrequest_to_user(sellrequest : dict, user : dict , relationship_meta : dict = None) :
    if relationship_meta == None :
        relationship_meta = dict()
    neo = get_neo()
    cypher_queries: list = []
    cypher_queries.append(neo.connect_nodes_makeCypherQuery(
        sourceNodeNames          = ['User'],
        sourceNodeMeta           = all_objectids_to_str(user),
        sourceVariableName       = 'U1',
        relationshipName         = 'WANTS_TO',
        relationShipMeta         = all_objectids_to_str(relationship_meta),
        relationshipVariableName = 'R1',
        targetNodeNames          = ['SellRequest'],
        targetNodeMeta           = all_objectids_to_str(sellrequest),
        targetVariableName       = 'S1'))

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    neo.write_multi(cypher_queries)

def driverequest_to_user(driverequest : dict, user : dict , relationship_meta : dict = None) :
    if relationship_meta == None :
        relationship_meta = dict()
    neo = get_neo()
    cypher_queries: list = []
    cypher_queries.append(neo.connect_nodes_makeCypherQuery(
        sourceNodeNames          = ['User'],
        sourceNodeMeta           = all_objectids_to_str(user),
        sourceVariableName       = 'U1',
        relationshipName         = 'WANTS_TO',
        relationShipMeta         = all_objectids_to_str(relationship_meta),
        relationshipVariableName = 'R1',
        targetNodeNames          = ['DriveRequest'],
        targetNodeMeta           = all_objectids_to_str(driverequest),
        targetVariableName       = 'D1'))

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    neo.write_multi(cypher_queries)

def loc_to_graph(loc : dict, loc_meta : dict = None, geometa : dict = None) -> str:
    """

    :param loc:
    :param loc_meta:
    :param geometa:
    :return:
    """
    if loc_meta == None :
        loc_meta = dict()
    if geometa == None :
        geometa = dict()
    neo = get_neo()
    location_name : str = ''
    cypher_queries: list = []
    if 'country' in loc and loc['country'] != '' and \
        'county' in loc and loc['county'] != '':

        merged_source_meta = merge_dicts({'name': loc['county']} , geometa)
        if not merged_source_meta :
            raise Exception('loc_to_graph :: failed to create source-meta for county')
        merged_target_meta = merge_dicts({'name': loc['country']}, geometa)
        if not merged_target_meta :
            raise Exception('loc_to_graph :: failed to create target-meta for county')

        cypher_queries.append(neo.connect_nodes_makeCypherQuery(
            sourceNodeNames          = ['Geo', 'County'],
            sourceNodeMeta           = merged_source_meta,
            sourceVariableName       = 's1',
            relationshipName         = 'IS_LOCATED_IN',
            relationShipMeta         = loc_meta,
            relationshipVariableName = 'r1',
            targetNodeNames          = ['Geo', 'Country'],
            targetNodeMeta           = merged_target_meta,
            targetVariableName       = 't1'))

        if 'municipality' in loc and loc['municipality'] != '':

            merged_source_meta = merge_dicts({'name': loc['municipality']} , geometa)
            if not merged_source_meta:
                raise Exception('loc_to_graph :: failed to create source-meta for municipality')
            merged_target_meta = merge_dicts({'name': loc['county']} , geometa)
            if not merged_target_meta:
                raise Exception('loc_to_graph :: failed to create target-meta for municipality')

            cypher_queries.append(neo.connect_nodes_makeCypherQuery(
                sourceNodeNames          = ['Geo', 'Municipality'],
                sourceNodeMeta           = merged_source_meta,
                sourceVariableName       = 's2',
                relationshipName         = 'IS_LOCATED_IN',
                relationShipMeta         = loc_meta,
                relationshipVariableName = 'r2',
                targetNodeNames          = ['Geo', 'County'],
                targetNodeMeta           = merged_target_meta,
                targetVariableName       = 't2'))

            merged_source_meta = merge_dicts({'name': loc['postcode']}, geometa)
            if not merged_source_meta:
                raise Exception('loc_to_graph :: failed to create source-meta for postcode')
            merged_target_meta = merge_dicts({'name': loc['municipality']}, geometa)
            if not merged_target_meta:
                raise Exception('loc_to_graph :: failed to create target-meta for postcode')

            cypher_queries.append(neo.connect_nodes_makeCypherQuery(
                sourceNodeNames          = ['Geo', 'Postcode'],
                sourceNodeMeta           = merged_source_meta,
                sourceVariableName       = 's3',
                relationshipName         = 'IS_LOCATED_IN',
                relationShipMeta         = loc_meta,
                relationshipVariableName = 'r3',
                targetNodeNames          = ['Geo', 'Municipality'],
                targetNodeMeta           = merged_target_meta,
                targetVariableName       = 't3'))

            road: str = loc['village'] + '_' + str(loc['postcode']) + '_ROAD'
            if 'road' in loc and loc['road'] != '':
                road = loc['road'] + ' , ' + str(loc['postcode'])

            merged_source_meta = merge_dicts({'name': road}, geometa)
            if not merged_source_meta:
                raise Exception('loc_to_graph :: failed to create source-meta for road')
            merged_target_meta = merge_dicts({'name': loc['postcode']}, geometa)
            if not merged_target_meta:
                raise Exception('loc_to_graph :: failed to create target-meta for road')

            cypher_queries.append(neo.connect_nodes_makeCypherQuery(
                sourceNodeNames          = ['Geo', 'Road'],
                sourceNodeMeta           = merged_source_meta,
                sourceVariableName       = 's4',
                relationshipName         = 'IS_LOCATED_IN',
                relationShipMeta         = loc_meta,
                relationshipVariableName = 'r4',
                targetNodeNames          = ['Geo', 'Postcode'],
                targetNodeMeta           = merged_target_meta,
                targetVariableName       = 't4'))

            location_name = loc['name']
            merged_source_meta = merge_dicts(loc, geometa)
            if not merged_source_meta :
                raise Exception('loc_to_graph :: failed to create source-meta for location')
            merged_target_meta = merge_dicts({'name': road}, geometa)
            if not merged_target_meta:
                raise Exception('loc_to_graph :: failed to create target-meta for location')
            if merged_source_meta.get('name' , '') == '' :
                raise Exception('loc_to_graph :: Nameless node for Location')

            cypher_queries.append(neo.connect_nodes_makeCypherQuery(
                sourceNodeNames          = ['Location'],
                sourceNodeMeta           = merged_source_meta,
                sourceVariableName       = 's5',
                relationshipName         = 'IS_LOCATED_IN',
                relationShipMeta         = loc_meta,
                relationshipVariableName = 'r5',
                targetNodeNames          = ['Geo', 'Road'],
                targetNodeMeta           = merged_target_meta,
                targetVariableName       = 't5'))

    if not cypher_queries or len(cypher_queries) <= 0 :
        print('WARNING :: loc_to_graph did not produce any queries')
        return ""

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    neo.write_multi(cypher_queries)
    return location_name

def get_all_countys() :
    """

    :return:
    """
    return get_neo().run_query('MATCH (g:County) RETURN g', ['g'])

def get_all_municipalities(county) :
    """

    :param county:
    :return:
    """

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    if isinstance(county , str) :
        return get_neo().search('MATCH (m:Municipality) -[:IS_LOCATED_IN]-> (:County ' + \
                                to_relaxed_json_str({'name' : county}) + ') RETURN m')
    elif isinstance(county , dict) :
        return get_neo().search('MATCH (m:Municipality) -[:IS_LOCATED_IN]-> (:County ' + \
                                to_relaxed_json_str(county) + ') RETURN m')
    else:
        raise Exception('Invalid argument type for county')

def get_all_nodes_in_municipality(county : str, muni : str, node : str) :
    """

    :param county:
    :param muni:
    :param node:
    :return:
    """

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    return get_neo().search(
        'MATCH (b:' + node + ') '
            '-[:IS_LOCATED_IN]-> (:Location) ' 
            '-[:IS_LOCATED_IN]-> (:Road) ' 
            '-[:IS_LOCATED_IN]-> (:Postcode) ' 
            '-[:IS_LOCATED_IN]-> (:Municipality ' + to_relaxed_json_str({'name' : muni})   + ') ' +
            '-[:IS_LOCATED_IN]-> (g:County '      + to_relaxed_json_str({'name' : county}) + ') ' +
        'RETURN b')

def get_all_nodes_in_county(county : str, node : str) :
    """

    :param county:
    :param node:
    :return:
    """

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    return get_neo().search(
        'MATCH (b:' + node + ') '
                             '-[:IS_LOCATED_IN]-> (:Location) '
                             '-[:IS_LOCATED_IN]-> (:Road) '
                             '-[:IS_LOCATED_IN]-> (:Postcode) '
                             '-[:IS_LOCATED_IN]-> (:Municipality) '
                             '-[:IS_LOCATED_IN]-> (g:County ' + to_relaxed_json_str({'name': county}) + ') ' +
        'RETURN b')

def get_all_roads_in_postcode(postcode : str) :
    """

    :param postcode:
    :return:
    """

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    return get_neo().search(
        'MATCH (r:Road) '
            '-[:IS_LOCATED_IN]-> (:Postcode ' + to_relaxed_json_str({'name': postcode}) + ') ' +

            # POSTCODES are unique !! no need to filter by county or municipality

            # '-[:IS_LOCATED_IN]-> (:Municipality ' + to_relaxed_json_str({'name': muni}) + ')' +
            # '-[:IS_LOCATED_IN]-> (:County ' + to_relaxed_json_str({'name': county}) + ')' +
        'RETURN r')

def get_all_postcodes_in_municipality(county : str, muni : str) :
    """

    :param county:
    :param muni:
    :return:
    """

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    return get_neo().search(
        'MATCH (p:Postcode) '
            '-[:IS_LOCATED_IN]-> (:Municipality ' + to_relaxed_json_str({'name': muni}) + ') ' +
            '-[:IS_LOCATED_IN]-> (:County ' + to_relaxed_json_str({'name': county}) + ') ' +
        'RETURN p')

def get_all_buyreqs_in_municipality(county : str, muni : str) :
    """

    :param county:
    :param muni:
    :return:
    """

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    return get_all_nodes_in_municipality(county, muni, 'BuyReq')

def get_all_sellreqs_in_municipality(county : str, muni : str) :
    """

    :param county:
    :param muni:
    :return:
    """

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    return get_all_nodes_in_municipality(county, muni, 'SellReq')

def get_all_transport_in_municipality(county : str, muni : str) :
    """

    :param county:
    :param muni:
    :return:
    """

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    return get_all_nodes_in_municipality(county, muni, 'Transport')

def get_all_transport_in_county(county : str) :
    """

    :param county:
    :return:
    """

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    return get_all_nodes_in_county(county, 'Transport')

def delete_and_detach(name : str, query : dict) :
    """

    :param name:
    :param query:
    :return:
    """

    # TODO
    # TODO : Use get_session instead of search, and used parametrised query building instead of string concatenation
    # TODO

    return get_neo().delete_node(name, query)
