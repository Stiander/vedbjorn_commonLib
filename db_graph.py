"""
    File : db_graph.py
    Author : Stian Broen
    Date : 15.04.2022
    Description :

Contains functions for operating on a Neo4J graph database

"""
import time
import inspect
from neo4j import GraphDatabase
import os

def to_relaxed_json_str(s) -> str :
    """

    :param s:
    :return:
    """
    if not s:
        return '{}'

    ret : str = ''

    def _VALUE_TO_STR(item) -> str :
        if isinstance(item, dict):
            return to_relaxed_json_str(item)
        elif isinstance(item, list):
            return to_relaxed_json_str(item)
        elif isinstance(item, str):
            return '\"' + str(item) + '\"'
        elif (isinstance(item, int) or isinstance(item, float)) :
            return str(item)
        else:
            raise Exception('Not Cypher-serializable')

    if isinstance(s , dict) :
        ret = ret + '{'
        for key , val in s.items() :
            ret = ret + key + ':' + _VALUE_TO_STR(val)
            ret = ret + ','
        ret = ret[:len(ret) - 1]
        ret = ret + '}'

    elif isinstance(s, list) :
        ret = ret + '['
        for item in s :
            ret = ret + _VALUE_TO_STR(item)
            ret = ret + ','
        ret = ret[:len(ret) - 1]
        ret = ret + ']'

    else:
        raise Exception('Not Cypher-serializable')

    return ret

def neo4j_robust_call(max_retries: int = 10):
    """

    :param max_retries:
    :return:
    """
    def decorator(func):
        """

        :param func:
        :return:
        """
        def wrapper(*args, **kwargs):
            """

            :param args:
            :param kwargs:
            :return:
            """
            stack = list(inspect.stack())
            if len(stack) >= 3 :
                print(stack[2].function, '::', stack[1].function)
            num_attempt: int = 0
            while num_attempt < max_retries:
                num_attempt = num_attempt + 1
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print('Neo4J Exception : ', e)
                time.sleep(1)
            raise Exception('Please read the exceptions from the logs for more information')
        return wrapper
    return decorator

class GraphDB:
    def __init__(self , verbose : bool = True):
        """

        :param verbose:
        """
        self.verbose = verbose
        self.pw = os.getenv('NEO4J_PW', 'letmein')
        self.username = os.getenv('NEO4J_USERNAME', 'neo4j')
        self.url = os.getenv('NEO4J_URL', 'bolt://neo4j:7687')

        print('NEO4J_URL : ' , self.url)

        self.db = GraphDatabase.driver(self.url, auth=(self.username, self.pw))
        with self.db.session() as session:
            session.write_transaction(self.create_and_return_greeting, 'hello world')
        self.delete_node('Greeting')
        print('Neo4J Connected')

    def create_and_return_greeting(self, tx, message):
        """

        :param tx:
        :param message:
        :return:
        """
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

    @neo4j_robust_call()
    def delete_node(self , name : str , query : dict = None, do_detach : bool = True):
        """

        :param name:
        :param query:
        :param do_detach:
        :return:
        """
        if not query :
            query = {}
        cypherQuery : str = 'MATCH (target:' + name + ' ' + to_relaxed_json_str(query) + ')\n'
        if do_detach :
            cypherQuery = cypherQuery + 'DETACH DELETE target'
        else:
            cypherQuery = cypherQuery + 'DELETE target'
        session = self.get_session()
        with session.begin_transaction() as tx:
            return tx.run(cypherQuery)

    @neo4j_robust_call()
    def run_query(self, query, get_attrs : list, *args, **kwargs):
        """

        :param query:
        :param get_attrs:
        :param args:
        :param kwargs:
        :return:
        """
        session = self.get_session()
        with session.begin_transaction() as tx:
            result = tx.run(query, *args, **kwargs)
            ret: list = []
            for record in result:
                if len(get_attrs) <= 0:
                    ret.append([dict(record)])
                else:
                    itms : list = []
                    for attr in get_attrs :
                        _rec = dict(record)
                        if attr in _rec :
                            itms.append(dict(_rec[attr]))
                    ret.append(itms)
            return ret

    @neo4j_robust_call()
    def search(self, query: str):
        """

        :param query:
        :return:
        """
        session = self.get_session()
        with session.begin_transaction() as tx:
            ret: list = []
            result = tx.run(query)
            for ix, record in enumerate(result):
                values = record.values()
                objs: list = []
                for node in values:
                    keys = node.keys()
                    obj: dict = {}
                    for key in keys:
                        obj[key] = node[key]
                    objs.append(obj)
                ret.append(objs)
            return ret

    def get_session(self):
        """

        :return:
        """
        with self.db.session() as session:
            return session

    def connect_nodes_makeCypherQuery(self,
         sourceNodeNames : [], sourceNodeMeta : dict , sourceVariableName : str ,
         relationshipName : str, relationShipMeta : dict , relationshipVariableName : str ,
         targetNodeNames : [], targetNodeMeta : dict , targetVariableName : str ,
         mergeSourceNode : bool = True, mergeTargetNode : bool = True, mergeRelationship : bool = True) -> str:
        """

        :param sourceNodeNames:
        :param sourceNodeMeta:
        :param sourceVariableName:
        :param relationshipName:
        :param relationShipMeta:
        :param relationshipVariableName:
        :param targetNodeNames:
        :param targetNodeMeta:
        :param targetVariableName:
        :param mergeSourceNode:
        :param mergeTargetNode:
        :param mergeRelationship:
        :return:
        """

        sourceNodeName: str = ''
        for i in range(0, len(sourceNodeNames)):
            sourceNodeName = sourceNodeName + sourceNodeNames[i]
            if (i + 1) < len(sourceNodeNames):
                sourceNodeName = sourceNodeName + ':'

        targetNodeName: str = ''
        for i in range(0, len(targetNodeNames)):
            targetNodeName = targetNodeName + targetNodeNames[i]
            if (i + 1) < len(targetNodeNames):
                targetNodeName = targetNodeName + ':'

        cypherQuery: str = ''
        if mergeSourceNode:
            sourceOp: str = 'MERGE'
        else:
            sourceOp: str = 'MATCH'
        cypherQuery = cypherQuery + sourceOp + ' (' + sourceVariableName + ': ' + sourceNodeName + ' ' + \
                      to_relaxed_json_str(sourceNodeMeta) + ') \n'

        if mergeTargetNode:
            targetOp: str = 'MERGE'
        else:
            targetOp: str = 'MATCH'
        cypherQuery = cypherQuery + targetOp + ' (' + targetVariableName + ': ' + targetNodeName + ' ' + \
                      to_relaxed_json_str(targetNodeMeta) + ') \n'

        if mergeRelationship:
            relationshipOp: str = 'MERGE'
        else:
            relationshipOp: str = 'CREATE'
        cypherQuery = cypherQuery + relationshipOp + ' (' + sourceVariableName + ')-[' + relationshipVariableName + ':' + relationshipName + ' ' + \
                      to_relaxed_json_str(relationShipMeta) + ']-> (' + targetVariableName +') \n'
        return cypherQuery

    @neo4j_robust_call()
    def write_multi(self, queries : list = None):
        """

        :param queries:
        :return:
        """
        if not queries :
            queries = []
        if len(queries) <= 0:
            return
        query : str = ''
        for part in queries :
            query = query + part + '\n'
        query = query[:len(query) - 1]

        session = self.get_session()
        with session.begin_transaction() as tx:
            return tx.run(query)

    @neo4j_robust_call()
    def connect_nodes(self , sourceNodeNames : [], sourceNodeMeta : dict , sourceVariableName : str ,
                      relationshipName : str, relationShipMeta : dict , relationshipVariableName : str ,
                      targetNodeNames : [], targetNodeMeta : dict , targetVariableName : str ,
                      mergeSourceNode : bool = True, mergeTargetNode : bool = True, mergeRelationship : bool = True):
        """

        :param sourceNodeNames:
        :param sourceNodeMeta:
        :param sourceVariableName:
        :param relationshipName:
        :param relationShipMeta:
        :param relationshipVariableName:
        :param targetNodeNames:
        :param targetNodeMeta:
        :param targetVariableName:
        :param mergeSourceNode:
        :param mergeTargetNode:
        :param mergeRelationship:
        :return:
        """

        query = self.connect_nodes_makeCypherQuery(
                sourceNodeNames, sourceNodeMeta, sourceVariableName,
                relationshipName, relationShipMeta, relationshipVariableName,
                targetNodeNames, targetNodeMeta, targetVariableName,
                mergeSourceNode, mergeTargetNode, mergeRelationship)

        session = self.get_session()
        with session.begin_transaction() as tx:
            return tx.run(query)
