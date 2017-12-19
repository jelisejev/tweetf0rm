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

		# bucket_folder = os.path.abspath('%s/%s'%(output_folder, bucket))
        #
		for k, lines in items.iteritems():
			entry = {
				"key": k,
				"values": map(lambda line: json.loads(line), lines)
			}
			col.insert_one(entry)

		# 	filename = os.path.abspath('%s/%s'%(bucket_folder, k))
		# 	with open(filename, 'ab+') as f:
		# 		for line in lines:
		# 			f.write('%s\n'%line)
        #
		# 	logger.debug("flushed %d lines to %s"%(len(lines), filename))

	except:
		logger.error(full_stack())

	return True

class MongoDBHandler(BaseHandler):

	def __init__(self, config):
		super(MongoDBHandler, self).__init__()
		self.config = config;


	def flush(self, bucket):
		self.client = MongoClient(self.config["url"])
		self.db = self.client[self.config["db"]]

		for bucket, items in self.buffer.iteritems():
			if (len(items) > 0):
				flush_row(self.db, bucket, items);

				self.clear(bucket)

		return True

	