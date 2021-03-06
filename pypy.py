import sqlite3
conn = sqlite3.connect('portfolio.db')

c = conn.cursor()

# Create table
c.execute('''CREATE TABLE deals
             (date text, trans text, symbol text, qty real, price real, brok text)''')

# Insert a row of data
c.execute("INSERT INTO deals VALUES ('2006-01-05','BUY','RHAT',100,35.14, 2)")

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()


