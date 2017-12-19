#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
write_to_handler.py: handler that's collects the data, and write to the disk on a separate thread; 

'''

import logging

logger = logging.getLogger(__name__)

from .base_handler import BaseHandler

from tweetf0rm.utils import full_stack
from pymongo import MongoClient
import json

def flush_row(db, bucket, items):
	try:
		col = db[bucket]

		for k, lines in items.iteritems():
			parsed_lines = map(lambda line: json.loads(line), lines)
			col.insert_many(parsed_lines)

	except:
		logger.error(full_stack())

	return True

FLUSH_SIZE = 100

class MongoDBHandler(BaseHandler):

	def __init__(self, config):
		super(MongoDBHandler, self).__init__()
		self.config = config

	def need_flush(self, bucket):
		if (len(self.buffer[bucket]) >  FLUSH_SIZE):
			return True
		else:
			return False


	def flush(self, bucket):
		self.client = MongoClient(self.config["url"])
		self.db = self.client[self.config["db"]]

		for bucket, items in self.buffer.iteritems():
			if (len(items) > 0):
				flush_row(self.db, bucket, items);

				self.clear(bucket)

		return True

	