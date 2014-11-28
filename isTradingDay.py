import datetime
import sqlite3
import calendar
import pdb

calendar.setfirstweekday(calendar.MONDAY)

def isWeekEnd(tDate):
        if tDate.weekday() == 5 or tDate.weekday() == 6:
		return True
	else:	return False

def isTradingDay(tDate):
	if isWeekEnd(tDate):
                return False
	else:	
		conn = sqlite3.connect('portfolio.db')
		c = conn.cursor()
		c.execute('SELECT COUNT(*) FROM calendar WHERE date = ?', (tDate.strftime("%Y-%m-%d"),))

		data = c.fetchone()[0]
		if data == 0:
			print('this is a trading day ' + tDate.isoformat())
			conn.close()			
			return True
		else:
			print('this is a HOLIDAY! ' + tDate.isoformat())
			conn.close()			
			return False

#q = datetime.datetime(2005,12,26)
#print isTradingDay(q)

def vTradingDates(stDate, endDate, cdr):
        conn = sqlite3.connect('portfolio.db', detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        c.execute('SELECT * FROM calendar WHERE (CDR=?) AND (date BETWEEN ? AND ?)', (cdr, stDate, endDate))
        holidays = []
        for row in list(c): holidays.append(row[0])

	step = datetime.timedelta(days=1)
	result = []
	while stDate < endDate:
                if not isWeekEnd(stDate):                       
                        if not stDate in holidays:
                                result.append(stDate)
                stDate += step
        conn.close()
	return result

def vTradingDates2(stDate, endDate):
	step = datetime.timedelta(days=1)
	result = []
	while stDate < endDate:
                if isTradingDay(stDate):
                        result.append(stDate.strftime('%Y-%m-%d %H:%M:%S'))
    		stDate += step
	return result

def getDateforYahoo(startD, endD):
        from datetime import timedelta
        result = [startD]
        for num in range(0, (endD - startD).days//365):
                result.append(result[-1] + timedelta(days=365))
        result.append(endD)        
        return result

def doRequestData(BBG, startD, endD):
        from yahoo_finance import Share
        from datetime import date

        if endD > date.today(): endD = date.today()

	conn = sqlite3.connect('portfolio.db', detect_types=sqlite3.PARSE_DECLTYPES)
	c = conn.cursor()

        tDate = vTradingDates(startD, endD, 'FR')

        c.execute('SELECT date FROM spots WHERE (date BETWEEN ? AND ?) AND (BBG=?)', (startD , endD, BBG))
        oDate = [i[0] for i in c.fetchall()]

        mDate = list(set(tDate) - set(oDate))
        mDate.sort()
        print "missing Dates", mDate

        if mDate:
                mfile = open("missingdates.csv", "w")
                convert_generator = (str(w)+';FR' for w in mDate)
                mfile.write('\n'.join(convert_generator))
                mfile.close()
                try: 
                        try: yahoo = Share(BBG)
                        except: print "yahoo = Share(?) failed...", BBG 
			
                        lDate = getDateforYahoo(mDate[0], mDate[-1])
                        print "ldate:", lDate, len(lDate)
                        
                        rslt = []
                        for i in range(0,  len(lDate)-1):
                                #pdb.set_trace()
                                print lDate[i], lDate[i+1]
                                try:
                                        rslt = rslt + yahoo.get_historical(lDate[i], lDate[i+1])
                                except:
                                        print "yahoo request failed:", BBG, lDate[i], lDate[i+1]
                        
                        for line in rslt: 
                                if 'Close' in line: c.execute('INSERT INTO spots VALUES(?, ?, ?, ?)', (BBG, line['Date'], float(line['Close']), 'close'))
                                if 'Open' in line: c.execute('INSERT INTO spots VALUES(?, ?, ?, ?)', (BBG, line['Date'], float(line['Open']), 'open'))
                                if 'High' in line: c.execute('INSERT INTO spots VALUES(?, ?, ?, ?)', (BBG, line['Date'], float(line['High']), 'high'))
                                if 'Low' in line: c.execute('INSERT INTO spots VALUES(?, ?, ?, ?)', (BBG, line['Date'], float(line['Low']), 'low'))
                                if 'Volume' in line: c.execute('INSERT INTO spots VALUES(?, ?, ?, ?)', (BBG, line['Date'], float(line['Volume']), 'volume'))
                except: print "Ops!! your request failed!"
        conn.commit()
        conn.close()

def cTurbo(Fwd, strike, barrier, quot, margin):
	if Fwd > strike: return (Fwd - strike)/quot + margin
	else: 	return 0

def pTurbo(Fwd, strike, barrier, quot, margin):
	if strike > Fwd: return (strike - Fwd)/quot + margin
	else: 	return 0

class Stock(object):
	"This Class holds historical stock information"	
	def __init__(self, BBG):
		self.mnemo = BBG 		
		conn = sqlite3.connect('portfolio.db', detect_types=sqlite3.PARSE_DECLTYPES)
		c = conn.cursor()
		try:
			c.execute("SELECT spot FROM spots WHERE BBG=? AND flag='close' AND date = (SELECT MAX(date) FROM spots WHERE BBG=? AND flag='close')", (self.mnemo, self.mnemo) )
			self.spot = c.fetchone()[0]
                        try:
                                d = conn.cursor()
                                c.execute("SELECT date, spot FROM spots WHERE BBG='" + self.mnemo + "' AND flag='close'" )
                                self.spots =  dict(c.fetchall())
                                #print self.spots[datetime.date(1999, 1, 5)]
                                #pdb.set_trace()
                        except: print "error in loading historic prices for " + self.mnemo
		except: self.spot = 0		

        def getClose(self, dDate):
                try : return self.spots[dDate]
                except : return "no close available for this stock at this date: "+ self.mnemo+" as of "+dDate.strftime("%Y-%m-%d")

        def getSpot(self):
                return self.spot

class Portfolio:
	def __init__(self):
		self.cash = 0

	def buy():
		return 0

	def sell():
		return 0

	def gValue():
		return 0

dt = datetime.date(1990, 03, 01)
end = datetime.date(2014, 11, 30)
	
if __name__=='__main__':
        from timeit import Timer
        t = Timer(lambda: vTradingDates(dt, end, 'FR'))
        #print t.timeit(number=1)
        print t.repeat(3, 5)
        #doRequestData('^FCHI', dt, end)
        #print vTradingDates(dt, end, 'FR')
	#print cTurbo(4346, 3750, 3750, 100.0, 0.08)
	#print pTurbo(4346, 4500, 4500, 100.0, 0.08)
	x = Stock("^FCHI")
        print x.spot
        dd = datetime.date(1999, 1, 5)
        print x.getClose(dd)
        #getDateforYahoo(dt, end)

#conn = sqlite3.connect('portfolio.db')
#c = conn.cursor()
# Create table
#c.execute('''CREATE TABLE deals
#             (date text, trans text, symbol text, qty real, price real, brok text)''')
#c.execute("INSERT INTO deals VALUES ('2006-01-05','BUY','RHAT',100,35.14, 2)")
#conn.commit()
#conn.close()
