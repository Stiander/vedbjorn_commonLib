###############################
#
# file          : pymongo_paginated_cursor.py
# author        : Oluwafemi Sule (stianb)
# date started  : 17.03.2018
# data finished : -
#
# description   :
#       https://stackoverflow.com/questions/49014697/pymongo-message-length-is-larger-than-server-max-message-size
#       Thank you Oluwafemi Sule
#
###############################

from itertools import count

class PaginatedCursor(object):
    def __init__(self, cur, limit=50):
        self.cur = cur
        self.limit = limit
        if hasattr(cur, 'count') and callable(cur.count):
            self.count = cur.count()
        elif hasattr(cur, 'collection') :
            coll = cur.collection
            if hasattr(coll, 'estimated_document_count') and callable(coll.estimated_document_count):
                self.count = coll.estimated_document_count()
            elif hasattr(coll, 'count_documents') and callable(coll.count_documents):
                self.count = coll.count_documents({})
            elif hasattr(coll, 'count') and callable(coll.count):
                self.count = coll.count()
            else:
                raise Exception('There is no way to know the amount of items in the cursor')
        else :
            raise Exception('There is no way to know the amount of items in the cursor')

    def __iter__(self):
        skipper = count(start=0, step=self.limit)
        for skip in skipper:
            if skip >= self.count:
                break
            for document in self.cur.skip(skip).limit(self.limit):
                yield document
            self.cur.rewind()
