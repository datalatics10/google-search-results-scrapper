import sqlite3

class ScrapperDB(object):
	"""docstring for scrapperDB"""
	def __init__(self):
		super(ScrapperDB, self).__init__()

		self.conn = sqlite3.connect('scrapperDB.db')
		self.c = self.conn.cursor()
		self.c.execute("CREATE TABLE IF NOT EXISTS URL(url_ID INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL, hashedText TEXT, location TEXT);")
		self.c.execute('CREATE TABLE IF NOT EXISTS MODIFIED_HISTORY(modified_ID INTEGER PRIMARY KEY AUTOINCREMENT, url_ID INTEGER NOT NULL, modified DATE NOT NULL, FOREIGN KEY(url_ID) REFERENCES URL(url_ID));')
		self.c.close()
		self.conn.close()


	def OpenConnection(self):
		self.conn = sqlite3.connect('scrapperDB.db')
		self.c = self.conn.cursor()


	def CloseConnection(self):
		self.c.close()
		self.conn.close()


	def  URL_insert(self, purl, phashedText, plocation, pmodified):
		self.c.execute('SELECT * from URL where url = ? LIMIT 0,1', (purl,))
		rows = self.c.fetchall()
		if len(rows)>0: 
			url_ID = rows[0][0]
			data = (phashedText, plocation, url_ID)
			self.c.execute('UPDATE URL SET hashedText=?,location=? WHERE url_ID=?', data)
		else: 
			data = (purl, phashedText, plocation)
			self.c.execute('INSERT INTO URL (url,hashedText,location) VALUES (?,?,?)', data)
			url_ID = self.c.lastrowid

		self.conn.commit()
		self.MODIFIED_HISTORY_insert(url_ID, pmodified)
		return;


	def URL_select(self, purl, phashedText):
	
		data = (purl, phashedText)
		self.c.execute('SELECT * from URL where url = ? AND hashedText = ?', data)
		rows = self.c.fetchall()
		return rows;


	def MODIFIED_HISTORY_insert(self, urlID, pmodified):
		data = (urlID,pmodified)
		self.c.execute('INSERT INTO MODIFIED_HISTORY (url_ID,modified) VALUES (?,?)',data)
		self.conn.commit()
		return;


	def MODIFIED_HISTORY_select(self, urlID):
		data = (urlID,)
		self.c.execute('SELECT * from MODIFIED_HISTORY where url_ID = ?', data)
		rows = self.c.fetchall()
		return rows;


	def All_URL_Details(self):
		self.c.execute("SELECT u.url_ID, u.url, u.hashedText, u.location, m.modified FROM URL u, MODIFIED_HISTORY m where u.url_ID = m.url_ID;")
		return rows;


	def URL_Details(self, purl, phashedText):
		self.c.execute("SELECT u.url_ID, u.url, u.hashedText, u.location, m.modified FROM URL u, MODIFIED_HISTORY m where u.url = ? And u.hashedText = ? AND u.url_ID = m.url_ID;", (purl,phashedText))
		rows = self.c.fetchall()
		return rows;