import sys, MySQLdb, pymssql, string, re

class DB(object):
	"""Class for connecting and interacting with databases (MySQL, MariaDB, MSSQL)"""
	def __init__(self, host=None, username=None, password=None, database=None, type=None, port=None, autocommit=False):
		self.host = host
		self.user = username
		self.pword = password
		self.dbase = database
		self.connection = None
		self.cursor = None
		self.type = type
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
			return False
		return True

	def connect(self, host=None, username=None, password=None, database=None, port=None, autocommit=None):
		"""establish connection and cursor, return boolean of success"""
		if self.__class__.__name__ in ('MySQL', 'MSSQL') and self.type != self.__class__.__name__:
			self.type = self.__class__.__name__
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
		if not self.type:
			return False
		if self.type == 'MySQL':
			try:
				self.connection = MySQLdb.connect(host=self.host, user=self.user, passwd=self.pword, port=self.port, db=self.dbase, charset='utf8')
				if self._autocommit:
					self.connection.autocommit(True)
			except:
				return False
		elif self.type == 'MSSQL':
			try:
				self.connection = pymssql.connect(server=self.host, user=self.user, password=self.pword, database=self.dbase, port=self.port)
				if self._autocommit:
					self.connection.autocommit(True)
			except:
				return False
		self.cursor = self.connection.cursor()
		return True

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

	def execute(self, qry, commit=False):
		"""execute given query, return can vary based on query type"""
		self.qry = qry
		if self.__class__.__name__ in ('MySQL', 'MSSQL') and self.type != self.__class__.__name__:
			self.type = self.__class__.__name__
		clue = qry.lower()[:6]
		if clue == 'insert':
			return self.insert(qry, commit)
		elif clue == 'select' or clue[:4] == 'show':
			return self.result(qry)
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
			if self.type == 'MySQL':
				affected = self.cursor.execute(qry)
			else:
				self.cursor.execute(qry)
				affected = self.cursor.rowcount
			if commit and not self._autocommit:
				self.connection.commit()
		except:
			return False
		return affected

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
		if not self.type:
			self.error = "no type"
			return False
		if clean:
			return self.qry_prep(self.prep_str(val))
		try:
			if self.type == 'MySQL':
				return str(val).replace('\\', '\\\\').replace("'", "\\'")
			elif self.type == 'MSSQL':
				return str(val).replace("'", "''")
			else:
				# unknown condition
				self.error = 'invalid type'
				return False
		except:
			# error occured
			self.error = 'could not edit %s' % val
			return False

	def result(self, qry):
		"""return tuple of SELECT query results"""
		try:
			self.cursor.execute(qry)
		except:
			self.error = 'query failed to execute'
			return False
		self.res = self.cursor.fetchall()
		return self.res

	def set_type(self, dbtype):
		"""set type of database to be used if not using included subclass"""
		prepped = dbtype.lower().strip()
		if prepped in ('mysql', 'mariadb'):
			if self.__class__.__name__ == 'MySQL':
				return True
			elif self.__class__.__name__ == 'MSSQL':
				return False
			self.type = 'MySQL'
			if type( self.port ) is not int:
				self.port = 3306
		elif prepped in ('ms', 'mssql', 'sql', 'sql server'):
			if self.__class__.__name__ == 'MSSQL':
				return True
			elif self.__class__.__name__ == 'MySQL':
				return False
			self.type = 'MSSQL'
			if type( self.port ) is not int:
				self.port = 1433
		else:
			return False
		return True

	def single(self, qry):
		"""return single scalar result from SELECT or similar query"""
		try:
			self.cursor.execute(qry)
		except:
			return False
		self.res = self.cursor.fetchall()
		if len( self.res ) > 0:
			return self.res[0][0]
		return None


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
