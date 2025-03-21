import sys, MySQLdb, pymssql, string, re
from collections import namedtuple

class DB(object):
	"""Class for connecting and interacting with databases (MySQL, MariaDB, MSSQL)"""

	_error = ''

	def __init__(self, host=None, username=None, password=None, database=None, type=None, port=None, autocommit=False):
		self.host = host
		self.user = username
		self.pword = password
		self.dbase = database
		self.connection = None
		self.cursor = None
		self.__type = type
		self.port = port
		self._autocommit = autocommit

	def __del__(self):
		"""gracefully close DB connection"""
		self.disconnect()

	def add(self, table, arg, v=False):
		"""build and execute INSERT query

		Attempts to insert a record into the named table translating k->v pairs of dictionary to column: value pairs

		Args:
			table: (string) name of database table
			args: (dict) keys are used as column names, values are inserted into associate column"""
		try:
			qry = "INSERT IGNORE INTO `%s` (`%%s`) VALUES (%%s)" % table
			cols = []
			vals = []
			for k in arg:
				cols.append(k)
				if arg[k] is None:
					vals.append('NULL')
				elif sys.version_info[0] == 2 and isinstance(arg[k], basestring):
					vals.append("'%s'" % self.qry_prep(arg[k], True))
				elif sys.version_info[0] == 3 and isinstance(arg[k], str):
					vals.append("'%s'" % self.qry_prep(arg[k], True))
				else:
					 vals.append( str(arg[k]) )
			qry = qry % ('`, `'.join(cols), ', '.join(vals))
			if v:
				print(qry)
			return self.insert( qry )
		except:
			self._error = 'insert failed'
			print(qry)
			return False

	def autocommit(self, val=True):
		"""set autocommit for an existing database connection"""
		self._autocommit = val
		try:
			if self.cursor:
				self.cursor.close()
				self.connection.autocommit(self._autocommit)
				self.cursor = self.connection.cursor()
		except:
			self._error = 'failed to set cursor to autocommit'
			return False
		return True

	def columns(self, table):
		if self.result("SELECT * FROM `%s` WHERE 0=1" % table) is False:
			self._error = 'failed to query `%s`' % table
			return False
		try:
			return tuple((x[0] for x in self.cursor.description))
		except:
			self._error = 'failed to get columns from `%s`' % table
			return False

	def connect(self, host=None, username=None, password=None, database=None, port=None, autocommit=None):
		"""establish connection and cursor, return boolean of success"""
		if self.__class__.__name__ in ('MySQL', 'MSSQL') and self.__type != self.__class__.__name__:
			self.__type = self.__class__.__name__
		if host != None:
			self.host = host
		if username != None:
			self.user = username
		if password != None:
			self.pword = password
		if database != None:
			self.dbase = database
		if port != None:
			_port = str(port).strip()
			if _port.isdigit():
				self.port = int(_port)
		# set to boolean
		self._autocommit = not not autocommit
		if not self.__type or self.__type not in ('MySQL', 'MSSQL'):
			self.__type = self.descendant_of(('MySQL', 'MSSQL'))
			if not self.__type:
				self._error = 'invalid type, must be MySQL or MSSQL'
				return False
		if self.__type == 'MySQL':
			try:
				self.connection = MySQLdb.connect(host=self.host, user=self.user, passwd=self.pword, port=self.port, db=self.dbase, charset='utf8')
				if self._autocommit:
					self.connection.autocommit(True)
			except:
				return False
		elif self.__type == 'MSSQL':
			try:
				self.connection = pymssql.connect(server=self.host, user=self.user, password=self.pword, database=self.dbase, port=self.port)
				if self._autocommit:
					self.connection.autocommit(True)
			except:
				return False
		self.cursor = self.connection.cursor()
		return True

	def descendant_of(self, _type):
		"""determine if instance is a descendant of MySQL or MSSQL
		if both are passed in a list or tuple the name of the one
		in the parentage is returned"""
		try:
			# specific instance for python2 usage allowing for any string type
			if isinstance(_type, basestring):
				return any(cls.__name__ == _type for cls in self.__class__.__mro__)
		except:
			pass
		if isinstance(_type, str):
			return any(cls.__name__ == _type for cls in self.__class__.__mro__)
		elif isinstance(_type, (list, tuple)):
			for type_name in _type:
				if any(cls.__name__ == type_name for cls in self.__class__.__mro__):
					return type_name
			else:
				return False
		return False

	def disconnect(self):
		"""break existing connection from database"""
		try:
			self.cursor.close()
			self.connection.close()
			self.cursor, self.connection = (None, None)
		except:
			return False
		return True
	# aliases
	close = disconnect
	stop = disconnect

	@property
	def error(self):
		return self._error

	def execute(self, qry, commit=False):
		"""execute given query, return can vary based on query type"""
		self.qry = qry
		if self.__class__.__name__ in ('MySQL', 'MSSQL') and self.__type != self.__class__.__name__:
			self.__type = self.__class__.__name__
		clue = qry.lower()[:6]
		if clue == 'insert':
			return self.insert(qry, commit)
		elif clue == 'select' or clue[:4] == 'show':
			return DB.result(self, qry)
		elif clue in ('update', 'delete'):
			return self.modify(qry, commit)
		try:
			# run query, leave next steps up to user
			self.cursor.execute(qry)
		except:
			return False
		return True

	def existing(self, col, table, distinct=False, conditions=None):
		"""return all existing values of a column in a particular table as a list"""
		try:
			qry = "SELECT "
			if distinct:
				qry += "DISTINCT "
			qry += "`%s` FROM `%s`" % (col, table)
			if conditions != None:
				if type( conditions ) is str:
					if conditions.strip().lower()[:5] == 'where':
						qry += ' %s' % conditions.strip()
				elif type( conditions ) is dict:
					where = []
					for k in conditions:
						try:
							where.append("`%s`='%s'" % (self.qry_prep(k), self.qry_prep(conditions[k], True)))
						except:
							pass
					if len(where) > 0:
						qry += ' WHERE %s' % ' AND '.join( where )
			self.res = self.result(qry)
			ret = []
			for row in self.res:
				ret.append( row[0])
			return ret
		except:
			return False

	def insert(self, qry, commit=False):
		"""execute an INSERT query and return the row ID if applicable"""
		try:
			self.cursor.execute(qry)
			if commit and not self._autocommit:
				self.connection.commit()
		except:
			return False
		return self.cursor.lastrowid

	def modify(self, qry, commit=False):
		"""execute an UPDATE, MODIFY, or DELETE query and return affected row count"""
		try:
			if self.__type == 'MySQL':
				affected = self.cursor.execute(qry)
			else:
				self.cursor.execute(qry)
				affected = self.cursor.rowcount
			if commit and not self._autocommit:
				self.connection.commit()
		except:
			return False
		return affected

	def named_result(self, qry, retain=False):
		"""return result output rows as named tuples"""
		return DB.result(self, qry, True, retain)

	@staticmethod
	def prep_str(raw):
		"""replace nonstandard and unicode characters with either standard or HTML encoded alternatives"""
		if not type(raw) is str:
			try:
				raw = str( raw )
			except:
				pass
		ascii_chars = set(string.printable)
		try:
			ret = raw.decode('utf-8').replace(u'\xba', '&#186;').replace(u'\xb0', '&#176;').replace(u'\xfc', '&#252;').replace(u'\xb2', '&#178;').replace(u'\xa0', u' ')\
			.replace(u'\xc9', '&#201;').replace(u'\xe9', '&#233;').replace(u'\xe5', '&#229;').replace(u'\xe2', '&#226;').replace(u'\xae', '&#174;').replace(u'\xe2\x80\x9d', '"')\
			.replace(u'\x99', '&#153;').replace(u'\u2122', '&#153;').replace(u'\u201c', '"').replace(u'\u201d', '"').replace(u'\u2018', "'").replace(u'\u2019', "'")\
			.replace(u'\u2020', '&#134;').replace(u'\u02dd','"').replace(u'\ufffd', '-').replace(u'\u00d7', '&#215;').replace(u'\u00d8', '&#216;').replace(u'\u044c', '&#216;')\
			.replace(u'\u0432', '&#215;').replace(u'\u00bd','&#189;').replace(u'\u00f8','&#xF8;').replace(u'\u5408','&#xd7;').replace(u'\xc3\xa2\xe2\x82\xac\xe2\x80\x9c', '-')\
			.replace(u'\xe2\x80\x93', '-').replace(u'\xe2\x80\x9d', '"').replace(u'\xe2\x80\x9c', '"').replace(u'\u00a6', '|').replace(u'\xe2\x80\x94', '-').replace(u'\u2014', '-')\
			.replace(u'\u2022', '&bull;').replace(u'\u00D6', '&#214;').replace(u'\u00A0', '&nbsp;').replace(u'\u2013', '-').replace(u'\u00B7', '&#183;').replace(u'\u00BC', '&#188;')\
			.replace(u'\u00BD', '&#189;').replace(u'\u00BE', '&#190;').replace(u'\u00A9', '&copy;').replace(u'\u00AD', '-').replace(u'\u2026', '&mldr;').replace(u'\u00B1', '&#177;')\
			.replace(u'\u201A', '&sbquo;').replace(u'\u00C2', '&#194;').replace(u'\u00C3', '&#195;').replace(u'\u00E2', '&#226;').replace(u'\u20AC', '&euro;').replace(u'\u026C', '')\
			.replace(u'\u271D ', '&#x271D;').replace(u'\u019A', '').replace(u'\uFB01', 'fi').replace(u'\uFB02', 'fl').replace(u'\u2033', '"').replace(u'\u00E4', '&#xe4;')\
			.replace(u'\u215B', '&frac18;').replace(u'\u00B4', "'").replace(u'\u00B0', '&deg;').replace(u'\u02DA', '&deg;').replace(u'\u2044', '/')
		except:
			ret = raw.replace(u'\xba', '&#186;').replace(u'\xb0', '&#176;').replace(u'\xfc', '&#252;').replace(u'\xb2', '&#178;').replace(u'\xa0', u' ')\
			.replace(u'\xc9', '&#201;').replace(u'\xe9', '&#233;').replace(u'\xe5', '&#229;').replace(u'\xe2', '&#226;').replace(u'\xae', '&#174;').replace(u'\xe2\x80\x9d', '"')\
			.replace(u'\x99', '&#153;').replace(u'\u2122', '&#153;').replace(u'\u201c', '"').replace(u'\u201d', '"').replace(u'\u2018', "'").replace(u'\u2019', "'")\
			.replace(u'\u2020', '&#134;').replace(u'\u02dd','"').replace(u'\ufffd', '-').replace(u'\u00d7', '&#215;').replace(u'\u00d8', '&#216;').replace(u'\u044c', '&#216;')\
			.replace(u'\u0432', '&#215;').replace(u'\u00bd','&#189;').replace(u'\u00f8','&#xF8;').replace(u'\u5408','&#xd7;').replace(u'\xc3\xa2\xe2\x82\xac\xe2\x80\x9c', '-')\
			.replace(u'\xe2\x80\x93', '-').replace(u'\xe2\x80\x9d', '"').replace(u'\xe2\x80\x9c', '"').replace(u'\u00a6', '|').replace(u'\xe2\x80\x94', '-').replace(u'\u2014', '-')\
			.replace(u'\u2022', '&bull;').replace(u'\u00D6', '&#214;').replace(u'\u00A0', '&nbsp;').replace(u'\u2013', '-').replace(u'\u00B7', '&#183;').replace(u'\u00BC', '&#188;')\
			.replace(u'\u00BD', '&#189;').replace(u'\u00BE', '&#190;').replace(u'\u00A9', '&copy;').replace(u'\u00AD', '-').replace(u'\u2026', '&mldr;').replace(u'\u00B1', '&#177;')\
			.replace(u'\u201A', '&sbquo;').replace(u'\u00C2', '&#194;').replace(u'\u00C3', '&#195;').replace(u'\u00E2', '&#226;').replace(u'\u20AC', '&euro;').replace(u'\u026C', '')\
			.replace(u'\u271D ', '&#x271D;').replace(u'\u019A', '').replace(u'\uFB01', 'fi').replace(u'\uFB02', 'fl').replace(u'\u2033', '"').replace(u'\u00E4', '&#xe4;')\
			.replace(u'\u215B', '&frac18;').replace(u'\u00B4', "'").replace(u'\u00B0', '&deg;').replace(u'\u02DA', '&deg;').replace(u'\u2044', '/')
		try:
			if sys.version_info[0] == 2:
				ret = ret.encode('utf-8')
				return re.sub(r'\s+', ' ', ''.join( filter(lambda x: x in ascii_chars, ret)) )
			else:
				return ret
		except:
			return re.sub(r'\s+', ' ', ''.join( filter(lambda x: x in ascii_chars, raw)) )

	def qry_prep(self, val, clean=False):
		"""add escape characters to string variables to be used in a query"""
		if not self.__type:
			self.error = "no type"
			return False
		if clean:
			return self.qry_prep(self.prep_str(val))
		try:
			if self.__type == 'MySQL':
				return str(val).replace('\\', '\\\\').replace("'", "\\'")
			elif self.__type == 'MSSQL':
				return str(val).replace("'", "''")
			else:
				# unknown condition
				self.error = 'invalid type'
				return False
		except:
			# error occured
			self.error = 'could not edit %s' % val
			return False

	def result(self, qry, named=False, retain=False):
		"""return tuple of SELECT query results"""
		try:
			self.cursor.execute(qry)
		except:
			self.error = 'query failed to execute'
			return False
		res = self.cursor.fetchall()
		if retain:
			self.res = res
		if named:
			try:
				if not res:
					return res
				Row = namedtuple('Row', list((x[0] for x in self.cursor.description)))
				ret = []
				for row in res:
					_row = Row(*row)
					ret.append(_row)
				return tuple(ret)
			except:
				return res
		return res

	def set_type(self, dbtype):
		"""set type of database to be used if not using included subclass"""
		prepped = dbtype.lower().strip()
		if prepped in ('mysql', 'mariadb'):
			if self.__class__.__name__ == 'MySQL':
				return True
			elif self.__class__.__name__ == 'MSSQL' or self.descendant_of('MSSQL'):
				return False
			self.__type = 'MySQL'
			if type( self.port ) is not int:
				self.port = 3306
		elif prepped in ('ms', 'mssql', 'sql', 'sql server'):
			if self.__class__.__name__ == 'MSSQL':
				return True
			elif self.__class__.__name__ == 'MySQL' or self.descendant_of('MySQL'):
				return False
			self.__type = 'MSSQL'
			if type( self.port ) is not int:
				self.port = 1433
		else:
			return False
		return True

	def single(self, qry, retain=False):
		"""return single scalar result from SELECT or similar query"""
		try:
			self.cursor.execute(qry)
		except:
			return False
		res = self.cursor.fetchall()
		if retain:
			self.res = res
		if len( res ) > 0:
			return res[0][0]
		return None

	@property
	def type(self):
		return self.__type

	@type.setter
	def type(self, dbtype):
		return self.set_type(dbtype)

class MySQL(DB):
	"""subclass of DB to interact with MySQL"""
	def __init__(self, host=None, username=None, password=None, database=None, port=3306, autocommit=False, autoconnect=True):
		if sys.version_info[0] > 2:
			super().__init__(host, username, password, database, 'MySQL', port, autocommit)
		else:
			# legacy code for Python2 usage
			super(MySQL, self).__init__(host=host, username=username, password=password, database=database, port=port, type='MySQL', autocommit=autocommit)
		if port != 3306:
			_port = str(port).strip()
			if _port.isdigit():
				self.port = int( _port)
			else:
				# set to default if invalid input provided
				self.port = 3306
		if autoconnect and host != None and username != None and password != None and database != None:
			self.connect()


class MSSQL(DB):
	"""subclass of DB to interact with MS SQL SERVER"""
	def __init__(self, host=None, username=None, password=None, database=None, port=1433, autocommit=False, autoconnect=True):
		if sys.version_info[0] > 2:
			super().__init__(host, username, password, database, 'MSSQL', port, autocommit)
		else:
			# legacy code for Python2 usage
			super(MSSQL, self).__init__(host=host, username=username, password=password, database=database, port=port, type='MSSQL', autocommit=autocommit)
		if port != 1433:
			_port = str(port).strip()
			if _port.isdigit():
				self.port = int( _port)
			else:
				# set to default if invalid input provided
				self.port = 1433
		if autoconnect and host != None and username != None and password != None and database != None:
			self.connect()
