import sys, MySQLdb, pymssql
import json, re, string
try:
	# only required for save_result method when saving output as XLSX file
	import xlsxwriter
except:
	pass
from collections import namedtuple

class DB(object):
	"""Class for connecting and interacting with databases (MySQL, MariaDB, MSSQL)"""

	_error = ''
	__type = None

	def __init__(self, host=None, username=None, password=None, database=None, type=None, port=None, autocommit=False, conn_timeout=None, qry_timeout=None):
		self.host = host
		self.user = username
		self.pword = password
		self.dbase = database
		self.connection = None
		self.cursor = None
		self.__type = type
		self.port = port
		self._autocommit = autocommit
		self.conn_timeout = conn_timeout or 30 # default for MySQL=10, MSSQL=60
		self.qry_timeout = qry_timeout or 0
		self._qry = None

	def __del__(self):
		"""gracefully close DB connection"""
		self.disconnect()

	def add(self, table, arg, commit=False, v=False):
		"""build and execute INSERT query

		Attempts to insert a record into the named table translating k->v pairs of dictionary to column: value pairs

		Args:
			table: (string) name of database table
			args: (dict) keys are used as column names, values are inserted into associate column"""
		try:
			qry = "INSERT IGNORE INTO %s (%%s) VALUES (%%s)" % self.prep_table_name(table)
			if self.__type == 'MSSQL':
				qry = qry.replace('IGNORE ', '')
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
					 vals.append(str(arg[k]))
			qry = qry % (self.prep_col_names(cols), ', '.join(vals))
			if v:
				print(qry)
			return self.insert(qry)
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
		if self.result("SELECT * FROM %s WHERE 0=1" % self.prep_table_name(table)) is False:
			self._error = 'failed to query `%s`' % table
			return False
		try:
			return tuple((x[0] for x in self.cursor.description))
		except:
			self._error = 'failed to get columns from %s' % self.prep_table_name(table)
			return False

	def connect(self, host=None, username=None, password=None, database=None, port=None, autocommit=None, conn_timeout=None, qry_timeout=None):
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
		if conn_timeout != None:
			self.conn_timeout = int(conn_timeout)
		if qry_timeout != None:
			self.qry_timeout = int(qry_timeout)
		if port != None:
			_port = str(port).strip()
			if _port.isdigit():
				self.port = int(_port)
		# set to boolean
		self._autocommit = not not autocommit
		try:
			if not self.__type or self.__type not in ('MySQL', 'MSSQL'):
				_type = self.descendant_of(('MySQL', 'MSSQL'))
				if not _type:
					self._error = 'invalid type, must be MySQL or MSSQL'
					return False
				self.set_type(_type)
		except:
			self.__type = self.descendant_of(('MySQL', 'MSSQL'))
		if not self.__type or self.__type not in ('MySQL', 'MSSQL'):
			return False
		if self.__type == 'MySQL':
			try:
				self.connection = MySQLdb.connect(host=self.host, user=self.user, passwd=self.pword, port=self.port, db=self.dbase, charset='utf8', connect_timeout=self.conn_timeout)
			except:
				return False
		elif self.__type == 'MSSQL':
			try:
				self.connection = pymssql.connect(server=self.host, user=self.user, password=self.pword, database=self.dbase, port=self.port, login_timeout=self.conn_timeout, timeout=self.qry_timeout)
			except:
				return False
		if self._autocommit:
			self.connection.autocommit(True)
		self.cursor = self.connection.cursor()
		if self.qry_timeout and self.__type == 'MySQL':
			self.execute("SET SESSION interactive_timeout=%d" % self.qry_timeout)
			self.execute("SET SESSION wait_timeout=%d" % self.qry_timeout)
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
		self._qry = qry
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

	def existing(self, col, table, distinct=False, conditions=None, v=False):
		"""return all existing values of a column in a particular table as a list"""
		try:
			qry = "SELECT "
			if distinct:
				qry += "DISTINCT "
			qry += "%s FROM %s" % (self.prep_col_names(col), self.prep_table_name(table))
			if conditions != None:
				if type(conditions) is str:
					if conditions.strip().lower()[:5] == 'where':
						qry += ' %s' % conditions.strip()
				elif type(conditions) is dict:
					where = []
					for k in conditions:
						try:
							where.append("`%s`='%s'" % (self.qry_prep(k), self.qry_prep(conditions[k], True)))
						except:
							pass
					if len(where) > 0:
						qry += ' WHERE %s' % ' AND '.join(where)
			if v:
				print(qry)
			self.res = self.result(qry)
			ret = []
			for row in self.res:
				ret.append(row[0])
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

	def paged_result(self, pg_num, qty_per_page, qry, named=False, retain=False):
		"""return pagenated results"""
		offset = int(qty_per_page)*(int(pg_num)-1)
		if self.__type == 'MySQL':
			if re.search(r'\s+LIMIT\s+\d.*$', qry, re.IGNORECASE):
				self._qry = re.sub(r'\s+LIMIT\s.+$', ' LIMIT %s, %s' % (offset, qty_per_page), qry)
			else:
				self._qry = '%s LIMIT %s, %s' % (qry.strip(' ;'), offset, qty_per_page)
		elif self.__type == 'MSSQL':
			if re.search(r'\s+OFFSET\s+\d.+$', qry, re.IGNORECASE):
				self._qry = re.sub(r'\s+OFFSET\s.+$', ' OFFSET %s ROWS FETCH NEXT %s ROWS ONLY' % (offset, qty_per_page), qry)
			else:
				self._qry = '%s OFFSET %s ROWS FETCH NEXT %s ROWS ONLY' % (qry.strip(' ;'), offset, qty_per_page)
		else:
			return None
		return DB.result(self, self._qry, named, retain)

	def prep_col_names(self, columns, bookended=False):
		"""wrap column name(s) in type dependent characters for building queries"""
		if self.__type == 'MySQL':
			_wrap = ('`','`')
		else:
			_wrap = ('[',']')
		glue = '%s, %s' % (_wrap[1], _wrap[0])
		cols = []
		if isinstance(columns, (list, tuple, dict)):
			for col in columns:
				cols.append(self.prep_col_names(col, True))
		else:
			if bookended:
				return columns
			else:
				return '%s%s%s' % (_wrap[0], columns.strip(' []`'), _wrap[1])
		if bookended:
			return glue.join(cols)
		return '%s%s%s' % (_wrap[0], glue.join(cols), _wrap[1])

	def prep_for_csv(self, raw, clean=True):
		if raw is None:
			return ''
		if isinstance(raw, (dict, list, tuple)):
			alt = json.dumps(raw)
		else:
			try:
				alt = self.prep_str(raw)
			except:
				alt = str(raw)
		if ',' in alt or '"' in alt:
			return '"%s"' % alt.replace('"', '""')
		return alt

	@staticmethod
	def prep_str(raw):
		"""replace nonstandard and unicode characters with either standard or HTML encoded alternatives"""
		if not type(raw) is str:
			try:
				raw = str(raw)
			except:
				pass
		ascii_chars = set(string.printable)
		try:
			ret = raw.decode('utf-8', errors='xmlcharrefreplace').replace(u'\xba', '&#186;').replace(u'\xb0', '&#176;').replace(u'\xfc', '&#252;').replace(u'\xb2', '&#178;').replace(u'\xa0', u' ')\
			.replace(u'\xc9', '&#201;').replace(u'\xe9', '&#233;').replace(u'\xe5', '&#229;').replace(u'\xe2', '&#226;').replace(u'\xae', '&#174;').replace(u'\xe2\x80\x9d', '"')\
			.replace(u'\x99', '&#153;').replace(u'\u2122', '&#153;').replace(u'\u201c', '"').replace(u'\u201d', '"').replace(u'\u2018', "'").replace(u'\u2019', "'")\
			.replace(u'\u2020', '&#134;').replace(u'\u02dd','"').replace(u'\ufffd', '-').replace(u'\u00d7', '&#215;').replace(u'\u00d8', '&#216;').replace(u'\u044c', '&#216;')\
			.replace(u'\u0432', '&#215;').replace(u'\u00bd','&#189;').replace(u'\u00f8','&#xF8;').replace(u'\u5408','&#xd7;').replace(u'\xc3\xa2\xe2\x82\xac\xe2\x80\x9c', '-')\
			.replace(u'\xe2\x80\x93', '-').replace(u'\xe2\x80\x9d', '"').replace(u'\xe2\x80\x9c', '"').replace(u'\u00a6', '|').replace(u'\xe2\x80\x94', '-').replace(u'\u2014', '-')\
			.replace(u'\u2022', '&bull;').replace(u'\u00D6', '&#214;').replace(u'\u00A0', '&nbsp;').replace(u'\u2013', '-').replace(u'\u00B7', '&#183;').replace(u'\u00BC', '&#188;')\
			.replace(u'\u00BD', '&#189;').replace(u'\u00BE', '&#190;').replace(u'\u00A9', '&copy;').replace(u'\u00AD', '-').replace(u'\u2026', '&mldr;').replace(u'\u00B1', '&#177;')\
			.replace(u'\u201A', '&sbquo;').replace(u'\u00C2', '&#194;').replace(u'\u00C3', '&#195;').replace(u'\u00E2', '&#226;').replace(u'\u20AC', '&euro;').replace(u'\u026C', '')\
			.replace(u'\u271D ', '&#x271D;').replace(u'\u019A', '').replace(u'\uFB01', 'fi').replace(u'\uFB02', 'fl').replace(u'\u2033', '"').replace(u'\u00E4', '&#xe4;')\
			.replace(u'\u215B', '&frac18;').replace(u'\u00B4', "'").replace(u'\u00B0', '&deg;').replace(u'\u02DA', '&deg;').replace(u'\u2044', '/').replace(u'\x91', "'")
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
			.replace(u'\u215B', '&frac18;').replace(u'\u00B4', "'").replace(u'\u00B0', '&deg;').replace(u'\u02DA', '&deg;').replace(u'\u2044', '/').replace(u'\x91', "'")
		try:
			if sys.version_info[0] == 2:
				ret = ret.encode('utf-8')
				return re.sub(r'\s+', ' ', ''.join(filter(lambda x: x in ascii_chars, ret)))
			else:
				return ret
		except:
			return re.sub(r'\s+', ' ', ''.join(filter(lambda x: x in ascii_chars, raw)))

	def prep_table_name(self, table, bookended=False):
		"""add type dependent wrap characters to database and table names for a query"""
		if self.__type == 'MySQL':
			_wrap = ('`','`')
		elif self.__type == 'MSSQL':
			_wrap = ('[',']')
		else:
			# no valid DB type set yet
			return table
		br = table.split('.')
		prepped = []
		try:
			for piece in br:
				prepped.append('%s%s%s' % (_wrap[0], piece.strip(' []`'), _wrap[1]))
			ret = '.'.join(prepped)
			if bookended:
				return ret.strip('[]`')
			return ret
		except:
			return table

	def qry_prep(self, val, clean=False):
		"""add escape characters to string variables to be used in a query"""
		if not self.__type:
			self._error = "no type"
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
				self._error = 'invalid type'
				return False
		except:
			# error occured
			self._error = 'could not edit %s' % val
			return False

	def result(self, qry, named=False, retain=False):
		"""return tuple of SELECT query results"""
		try:
			self.cursor.execute(qry)
		except:
			self._error = 'query failed to execute'
			return False
		res = self.cursor.fetchall()
		if retain:
			self.res = res
		if res and named:
			try:
				Row = namedtuple('Row', list((x[0] for x in self.cursor.description)))
				ret = []
				for row in res:
					_row = Row(*row)
					ret.append(_row)
				return tuple(ret)
			except:
				return res
		return res

	def row(self, qry, named=False, retain=False):
		if self._qry != qry:
			self._qry = qry
			self.cursor.execute(qry)
		res = self.cursor.fetchone()
		if res and named:
			try:
				Row = namedtuple('Row', list((x[0] for x in self.cursor.description)))
				return Row(*res)
			except:
				return res
		return res

	def save_result(self, res, dest, columns=None):
		"""write result object to a CSV or XLSX file"""
		try:
			if re.search(r'.+\.csv$', dest, re.IGNORECASE):
				# write to csv
				f = open(dest, 'w')
				for i, row in enumerate(res):
					if not i:
						# first row, add labels
						if isinstance(columns, (tuple, list)):
							if len(columns) != len(row):
								print('Column count mismatch')
								return False
							try:
								f.write('%s\n' % ','.join(list(map(self.prep_for_csv, columns))))
							except:
								print('Failed to write custom column names')
								return False
						else:
							try:
								# use labels from namedtuple as header
								f.write('"%s"\n' % '","'.join(list(map(self.prep_for_csv, row._fields))))
							except:
								print('Failed to read column names, provide columns value or use namedtuple for rows')
								return False
					try:
						# attempt to write row to file
						# even with namedtuple row.__getnewargs__() is not required
						f.write('%s\n' % ','.join(list(map(self.prep_for_csv, row))))
					except:
						# failed to write row
						print('failed to write row #%d' % i)
						print(row)
						break
				f.close()
			elif re.search(r'.+\.xlsx$', dest, re.IGNORECASE):
				# write to Excel file
				workbook = xlsxwriter.Workbook(dest, {'strings_to_urls': False})
				worksheet = workbook.add_worksheet('Sheet1')
				for i, row in enumerate(res):
					if not i:
						# first row, add labels
						if isinstance(columns, (tuple, list)):
							if len(columns) != len(row):
								print('Column count mismatch')
								return False
							try:
								for j, col in enumerate(columns):
									worksheet.write(i, j, col)
							except:
								print('Failed to write custom column names')
								return False
						else:
							# get column names from namedtuple labels
							try:
								for j, col in enumerate(row._fields):
									worksheet.write(i, j, col)
							except:
								print('Failed to read column names')
								return False
					try:
						# attempt to write row to file
						# even with namedtuple row.__getnewargs__() is not required
						for j, val in enumerate(row):
							try:
								worksheet.write(i+1, j, val)
							except:
								worksheet.write(i+1, j, str(val))
					except:
						# failed to write row
						print('failed to write row #%d' % i)
						print(row)
						break
				# freeze top row and add columnal filters
				worksheet.freeze_panes(1,0)
				worksheet.autofilter(0, 0, len(res), len(row)-1)
				workbook.close()
			else:
				print('invalid file type')
				return False
			return True
		except:
			print('Failed to save result to file')
			return False

	def set_type(self, dbtype):
		"""set type of database to be used, check if compatible subclass is being used"""
		prepped = dbtype.lower().strip()
		if prepped in ('mysql', 'mariadb'):
			if self.__class__.__name__ == 'MySQL':
				return True
			elif self.__class__.__name__ == 'MSSQL' or self.descendant_of('MSSQL'):
				return False
			self.__type = 'MySQL'
			if not hasattr(self, 'port') or type(self.port) is not int:
				self.port = 3306
		elif prepped in ('ms', 'mssql', 'sql', 'sql server'):
			if self.__class__.__name__ == 'MSSQL':
				return True
			elif self.__class__.__name__ == 'MySQL' or self.descendant_of('MySQL'):
				return False
			self.__type = 'MSSQL'
			if not hasattr(self, 'port') or type(self.port) is not int:
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
		if len(res) > 0:
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
	def __init__(self, host=None, username=None, password=None, database=None, port=3306, autocommit=False, autoconnect=True, conn_timeout=0, qry_timeout=0):
		if sys.version_info[0] > 2:
			super().__init__(host, username, password, database, 'MySQL', port, autocommit)
		else:
			# legacy code for Python2 usage
			super(MySQL, self).__init__(host=host, username=username, password=password, database=database, port=port, type='MySQL', autocommit=autocommit)
		if port != 3306:
			_port = str(port).strip()
			if _port.isdigit():
				self.port = int(_port)
			else:
				# set to default if invalid input provided
				self.port = 3306
		if autoconnect and host != None and username != None and password != None and database != None:
			self.connect()


class MSSQL(DB):
	"""subclass of DB to interact with MS SQL SERVER"""
	def __init__(self, host=None, username=None, password=None, database=None, port=1433, autocommit=False, autoconnect=True, conn_timeout=0, qry_timeout=0):
		if sys.version_info[0] > 2:
			super().__init__(host, username, password, database, 'MSSQL', port, autocommit)
		else:
			# legacy code for Python2 usage
			super(MSSQL, self).__init__(host=host, username=username, password=password, database=database, port=port, type='MSSQL', autocommit=autocommit)
		if port != 1433:
			_port = str(port).strip()
			if _port.isdigit():
				self.port = int(_port)
			else:
				# set to default if invalid input provided
				self.port = 1433
		if autoconnect and host != None and username != None and password != None and database != None:
			self.connect()
