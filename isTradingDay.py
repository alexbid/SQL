import datetime
import sqlite3
import calendar

calendar.setfirstweekday(calendar.MONDAY)

def isWeekEnd(tDate):
        if tDate.weekday() == 5 or tDate.weekday() == 6:
		#print('this is a WEEKEND ' + tDate.isoformat())		
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
#print q.today() 
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

dt = datetime.date(2014, 11, 01)
end = datetime.date(2014, 12, 30)
#print vTradingDates(dt, end, 'FR')

def doRequestData(BBG, startD, endD):
        from yahoo_finance import Share
        from datetime import date

        if endD > date.today(): endD = date.today()
	conn = sqlite3.connect('portfolio.db', detect_types=sqlite3.PARSE_DECLTYPES)
	c = conn.cursor()

        lDate = vTradingDates(startD, endD, 'FR')
        for rows in lDate:
                print rows
                c.execute('SELECT COUNT(date) FROM spots WHERE (date BETWEEN ? AND ?) AND (BBG=?)', (rows , rows, BBG))
		
                data = c.fetchone()[0]
                #for toto in data: print toto
		if data == 0:
                        try: 
                                yahoo = Share(BBG)
                                #print yahoo.get_open()
                                #print yahoo.get_trade_datetime()
                                #print rows
                                try:
                                        rslt =  yahoo.get_historical(rows, rows)
                                        #save le close
                                        if 'Close' in rslt: 
                                                print rslt['Close']
                                                print (BBG, rows, float(rslt['Close']), 'close') 
                                                try: 
                                                        c.execute('INSERT INTO spots VALUES(?, ?, ?, ?)', (BBG, rows, float(rslt['Close']), 'close'))
                                                except ValueError:
                                                        print ValueError
                                except: print rows
                                #c.execute('')
                        except:
                                print "Ops!! Check your Internet Connection or check your BBG!"
       
        conn.commit()
        conn.close()

        #for row in data: print row
        #conn.close()			
        
##doRequestData('^FCHI', dt, end)
if __name__=='__main__':
        from timeit import Timer
        t = Timer(lambda: vTradingDates(dt, end, 'FR'))
        #print t.timeit(number=1)
        print t.repeat(3, 5)
        doRequestData('^FCHI', dt, end)

#conn = sqlite3.connect('portfolio.db')
#c = conn.cursor()
# Create table
#c.execute('''CREATE TABLE deals
#             (date text, trans text, symbol text, qty real, price real, brok text)''')
#c.execute("INSERT INTO deals VALUES ('2006-01-05','BUY','RHAT',100,35.14, 2)")
#conn.commit()
#conn.close()


