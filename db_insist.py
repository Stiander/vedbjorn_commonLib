__author__ = 'Stiander'

from datetime import datetime
import time
from bson.objectid import ObjectId
from .pymongo_paginated_cursor import PaginatedCursor as mpcur
import pymongo
from pymongo import errors
import gridfs
import sys , os
from difflib import SequenceMatcher

DEFAULT_BATCH_SIZE = 40
SMALL_CHUNK_SUFF = '_chunks'
SHRINKED_KEY = 'shrinked'
MAX_DOCUMENT_SIZE=16793597

class DocumentSizeExceededException(Exception):
    def __init__ (self, size, time=datetime.now().strftime('%d.%m.%Y %H:%M:%S'), msg=f'is above the threshold limit of {MAX_DOCUMENT_SIZE}'):
        self.size = size
        self.time = time
        self.msg = msg
        super().__init__(self.msg)

    def __str__ (self):
        return f'{self.size} bytes {self.msg}. Time of incident {self.time}'

def ensure_col(col):
    if isinstance(col, pymongo.collection.Collection):
        return col.name
    else:
        return col

def all_objectids_to_str(_obj) :
    if isinstance(_obj , ObjectId) :
        return str(_obj)
    elif isinstance(_obj , datetime) :
        return _obj.strftime('%d.%m.%Y %H:%M:%S')
    elif isinstance(_obj , list) :
        ret : list = []
        for _item in _obj :
            ret.append(all_objectids_to_str(_item))
        return ret
    elif isinstance(_obj , dict) :
        ret : dict = {}
        for _key , _val in _obj.items() :
            ret[_key] = all_objectids_to_str(_val)
        return ret
    elif isinstance(_obj , tuple) :
        return [all_objectids_to_str(_obj[0]) , all_objectids_to_str(_obj[1])]
    else:
        return _obj

def get_most_likely_attribute(_obj : dict , _name : str , ignore : list = []) -> tuple: # confidence , key/value pair
    _val = None
    _confidence : float = 0.0
    a = _name.lower()
    if a == 'n/a' or a == 'na' :
        return 0 , 'N/A'
    for _key , _value in _obj.items() :
        b = str(_key).lower()
        cancel = False
        for _ig in ignore :
            if str(_ig) == b :
                cancel = True
                break
        if cancel:
            continue
        conf = SequenceMatcher(None, a, b).ratio()
        if conf > _confidence :
            _confidence = conf
            _val = { _key : _value}
        if isinstance(_value , dict) :
            _sub_c , _sub_kv = get_most_likely_attribute(_value , _name)
            if _sub_c > _confidence :
                _confidence = _sub_c
                _val = _sub_kv
    return _confidence , _val

def extract_value_from_obj(_obj) :
    if isinstance(_obj , dict) :
        for _key , _value in _obj.items() :
            if str(_key).lower() == 'value' and not isinstance(_value , dict) :
                return _value
            elif isinstance(_value , dict) :
                subv = extract_value_from_obj(_value)
                if subv :
                    return subv
    elif isinstance(_obj , str) :
        return _obj
    else:
        return str(_obj)
    return ''

def get_most_likely_real_value(look_in , candidates , req_conf : float = 0.75, default_str : str = '', ignore : list = None) :
    if isinstance(candidates , str) :
        conf, kp = get_most_likely_attribute(look_in, candidates, ignore)
        if conf >= req_conf :
            ret = extract_value_from_obj(kp)
            if ret:
                return ret
            else:
                return default_str
        else:
            return default_str
    elif isinstance(candidates , list) :
        results : dict = {}
        for name in candidates :
            conf, kp = get_most_likely_attribute(look_in, name, ignore)
            if conf >= req_conf:
                results[conf] = kp
        _max_conf = 0
        winner = None
        for _conf , kp in results.items() :
            if _conf > _max_conf :
                if extract_value_from_obj(kp) :
                    winner = kp
        if not winner :
            return default_str
        return extract_value_from_obj(winner)
    else:
        return default_str

"""
Convenice-function : Just set the graph-guide document's graph_changed attribute to True
"""
def set_graph_changed() :
    db = get_db()
    guide_doc = db.insist_on_find_one_q('matchloop_guide' , {})
    if not guide_doc :
        db.insist_on_insert_one('matchloop_guide', {
            'graph_changed' : True
        })
    else:
        db.insist_on_update_one(guide_doc, 'matchloop_guide', 'graph_changed', True)

class InsistDB:
    def __init__(self, _db = None):
        self.db = _db
        self.maxAttempts : int = 10
        if self.db == None:
            self.connect()

    def start_transaction(self):
        return dbTransaction(self.db)

    def is_ObjectId(self, val) -> bool:
        try:
            _id = ObjectId(val)
        except Exception:
            return False
        return True

    def connect(self):
        try:
            mongodb_uri = str(os.environ['MONGODB_URI'])
        except Exception:
            mongodb_uri = 'mongodb://localhost:27017/vedbjorn?ssl=false'

        attempts = 1
        while True:
            self.dbclient = pymongo.MongoClient(mongodb_uri)
            try:
                self.dbclient.admin.command('ismaster')
                print('\tConnected to MongoDB')
            except pymongo.errors.ConnectionFailure as err:
                print('Failed to connect to MongoDB on the ' + str(attempts) + ' attempt, error was : \n\t' + str(err) )
                time.sleep(attempts)
                attempts = attempts + 1
                if attempts > 10:
                    print('APPLICATION COULD NOT CONNECT TO MONGODB AND WILL THEREFORE SHUT DOWN IN 10 SECONDS')
                    time.sleep(10)
                    sys.exit('Application exit : Failed to connect to MongoDB')
                continue
            self.db = self.dbclient.get_database('vedbjorn')
            break

    def insist_on_aggregate(self, col, agg):
        return self.db[ensure_col(col)].aggregate(agg)

    def insist_on_find(self, col : str, query : dict = {} , proj = None , _limit : int = 0 , distinct : str = ''):
        if not distinct :
            if proj == None:
                if _limit <= 0:
                    return self.db[ensure_col(col)].find(query)
                return self.db[ensure_col(col)].find(query).limit(_limit)
            if _limit <= 0:
                return self.db[ensure_col(col)].find(query , proj)
            return self.db[ensure_col(col)].find(query , proj).limit(_limit)
        else:
            if proj == None:
                if _limit <= 0:
                    return self.db[ensure_col(col)].find(query).distinct(distinct)
                return self.db[ensure_col(col)].find(query).distinct(distinct).limit(_limit)
            if _limit <= 0:
                return self.db[ensure_col(col)].find(query , proj).distinct(distinct)
            return self.db[ensure_col(col)].find(query , proj).distinct(distinct).limit(_limit)

    def insist_on_find_most_recent(self, col : str, query : dict = {} , proj = None):
        return self.db[ensure_col(col)].find_one(query , proj, sort=[( '_id', pymongo.DESCENDING )])

    def insist_on_find_with_sort(self, col : str, query : dict = {} , proj = None , _limit : int = 0, orderby : dict = {}):
        if proj == None:
            if _limit <= 0:
                return self.db[ensure_col(col)].find(query).sort(orderby)
            return self.db[ensure_col(col)].find(query).sort(orderby).limit(_limit)
        if _limit <= 0:
            return self.db[ensure_col(col)].find(query , proj).sort(orderby)
        return self.db[ensure_col(col)].find(query , proj).sort(orderby).limit(_limit)

    def insist_on_find_one(self, col : str, _id , proj = None):
        return self.insist_on_find_one_q(col, {'_id': ObjectId(_id)} , proj)

    def insist_on_find_one_q(self, col : str, query : dict , proj = None):
        if proj == None:
            return self.db[ensure_col(col)].find_one(query)
        return self.db[ensure_col(col)].find_one(query , proj)

    def insist_on_update_many(self, ids : list, col : str, attributeName : str, attribute, operator :str = '$set'):
        return self.db[ensure_col(col)].update_many({'_id' : {'$in' : ids}} , {operator : {attributeName : attribute}})

    def insist_on_update_many_manual(self, query : dict, col : str, update: dict):
        return self.db[ensure_col(col)].update_many(query, update)

    def insist_on_increment(self, id, col: str, attributeName: str, num: float):
        return self.db[ensure_col(col)].update_one({'_id': ObjectId(id)} , {'$inc' : {attributeName : num}})

    def insist_on_update_one(self, obj : dict, col : str, attributeName : str, attribute, operator :str = '$set', doUpsert : bool = False):
        return self.insist_on_update_one_qi(ObjectId(obj['_id']), col, {attributeName : attribute}, operator, doUpsert)

    def insist_on_update_one_q(self, obj : dict, col : str, insert_q : dict, operator :str = '$set', doUpsert : bool = False):
        return self.insist_on_update_one_qi(ObjectId(obj['_id']), col, insert_q, operator, doUpsert)

    def insist_on_update_one_qi(self, id , col : str, insert_q : dict, operator :str = '$set', doUpsert : bool = False):
        return self.db[ensure_col(col)].update_one({'_id': ObjectId(id)}, {operator: insert_q}, upsert=doUpsert)

    def insist_on_update_one_q_manual(self, query: dict, col : str, update : dict, doUpsert : bool = False):
        return self.db[ensure_col(col)].update_one(query, update, upsert=doUpsert)

    def insist_on_insert_one(self, col : str, document : dict):
        size = sys.getsizeof(document)
        if size > MAX_DOCUMENT_SIZE:
            raise DocumentSizeExceededException(size)

        ret = self.db[ensure_col(col)].insert_one(document)
        if ret.acknowledged == True:
            return ret.inserted_id
        return None

    def insist_on_replace_one_q(self, col : str, query : dict, document : dict, doUpsert = False):
        return self.db[ensure_col(col)].replace_one(query, document, upsert=doUpsert)

    def insist_on_replace_one(self, col : str, _id, document : dict, doUpsert = False):
        return self.insist_on_replace_one_q(col, {'_id': ObjectId(_id)}, document, doUpsert)

    def insist_on_delete_one_q(self, col : str, query : dict):
        return self.db[ensure_col(col)].delete_one(query)

    def insist_on_delete_one(self, col : str, _id):
        return self.insist_on_delete_one_q(col, {'_id': ObjectId(_id)})

    def insist_on_insert_file(self, bytes : bytearray , filename : str , contentType : str , meta : dict ,
                              col : str = 'files'):
        gfiles = gridfs.GridFS(self.db, collection=col)
        gridfs_ret_id = gfiles.put(bytes, filename=filename, contentType=contentType, meta=meta)
        return gridfs_ret_id

    def insist_on_delete_file(self, col : str, _id):
        grfs = gridfs.GridFS(self.db, collection=ensure_col(col))
        grfs.delete(ObjectId(_id))

    def insist_on_get_filecontent(self, filename : str, col : str = 'files'):
        col = ensure_col(col)
        fileObj = self.insist_on_find_one_q('files.' + col, {'filename':filename})
        if fileObj == None:
            return None
        grfs = gridfs.GridFS(self.db, collection=col)
        fileGridObject = grfs.get(fileObj['_id'])
        content : bytes = fileGridObject.read()
        fileGridObject.close()
        return content

    def insist_on_get_filecontent_id(self, _id , col : str = 'files'):
        col = ensure_col(col)
        fileObj = self.insist_on_find_one('files.' + col, ObjectId(_id))
        if fileObj == None:
            return None
        grfs = gridfs.GridFS(self.db, collection=col)
        fileGridObject = grfs.get(fileObj['_id'])
        content : bytes = fileGridObject.read()
        fileGridObject.close()
        return content

    def insist_on_get_filechunk_ids(self, _id, col : str = 'files') -> dict:
        col = ensure_col(col)
        fileObj = self.insist_on_find_one('files.' + col, ObjectId(_id), {'_id' : 1})
        if fileObj == None:
            return dict()
        sort_me : dict = {}
        objs = self.insist_on_find(col + '.chunks', {'files_id' : fileObj['_id']}, {'_id': 1, 'n' : 1})
        for obj in mpcur(objs, limit=50):
            sort_me[obj['n']] = obj['_id']
        return_me : dict = {}
        for i in sorted(sort_me):
            return_me[i] = sort_me[i]
        return return_me

    def insist_on_delete_many(self, col: str, query: dict):
        col = ensure_col(col)
        objs = self.insist_on_find(col, query, {'_id': 1})
        delete_these: list = []
        for obj in mpcur(objs, limit=50):
            delete_these.append(obj['_id'])
        return self.insist_on_delete_many_ids(col, delete_these)

    def insist_on_delete_many_ids(self, col : str, ids : list):
        ret = self.db[col].delete_many({'_id': {'$in': ids}})
        return ret.deleted_count

    def insist_on_insert_many(self, col : str, items : list):
        self.db[ensure_col(col)].insert_many(items)

    def insist_on_append_to_list(self, id, col : str, attributeName : str, values : list):
        if len(values) <= 0:
            return
        query : dict = { attributeName : { '$each' : values } }
        return self.insist_on_update_one_qi(ObjectId(id), col, query, '$push')

    def insist_on_pull(self, id, col : str, arrayName : str, query):
        return self.insist_on_update_one_qi(ObjectId(id), col, { arrayName : query }, '$pull')

    def insist_on_remove_attribute(self, id, col : str, attributeName : str):
        query : dict = {attributeName:1}
        self.insist_on_update_one_qi(ObjectId(id), col, query, '$unset')

    def insist_on_set_attribute_in_array_at_index(self, id, col : str, index : int, arrayName : str,
                                                  attributeName: str , attributeValue) :
        return self.db[ensure_col(col)].update_one(
            {'_id' : ObjectId(id)} ,
            {'$set' : {
                arrayName + '.' + str(index) + '.' + attributeName : attributeValue
            }}
        )


class dbTransaction():
    def __init__(self, db : pymongo.mongo_client.database.Database):
        if type(db) is not pymongo.mongo_client.database.Database:
            raise Exception("Expected pymongo.database.Database")
        self.db = db

    def __enter__(self):
        self.client_session = self.db.client.start_session()
        self.mongo_transaction = self.client_session.start_transaction()
        return self

    def __exit__(self, type, value, traceback):
        if not self.client_session.has_ended:
            self.client_session.end_session()
        del self.mongo_transaction
        del self.client_session

    def commit(self):
        self.client_session.commit_transaction()

    def abort(self):
        self.client_session.abort_transaction()

db = None
def get_db() :
    global db
    if db == None:
        db = InsistDB()
    return db