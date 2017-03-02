from flask import Flask, render_template, request, redirect, url_for, session
import boto
import os
from boto import rds
import MySQLdb
import datetime
import string
import memcache
import time
import random
import csv
import hashlib
application = Flask(__name__)
app = application




@app.route('/')
def hello_word():
		
		
				
		
		return """ 
		
		<p> LAST NAME: JAYARAM, ID :3971, BATCH : 3.30
		
		<form id ="upload_file" enctype="multipart/form-data" method="post" action="/upload_file">

		<input id="fileupload" name="myfile" type="file" />
		
		<input type ="submit" value="submit" name ="submit"></form>
		<p> upload to S3</p>
		
		<form id ="upload_db" enctype="multipart/form-data" method="post" action="/upload_db">

		<input id="dbupload" name="mydb" type="file" />
		
		<input type ="submit" value="submit" name ="submit"></form>
		<p> upload to RDS</p>
		
		
		"""

		
@app.route('/upload_db', methods=['POST'])
def upload_db():
			
		file = request.files['mydb']
		filecontent=file.read()
		filename=file.filename
		
		fo = open(filename, "w")
		fo.write(filecontent);
		fo.close()
		coloumn =[]
		
		with open(filename, 'rb') as f:
			data = list(csv.reader(f))
		coloumn_first = data[0]
		
		for word in coloumn_first:
				if(word == "COUNT" or word == "Count" or word == "count"):
						word = "my_count"
				if(word == "Dec"):
						word ="decem"
				coloumn_second = word.replace(" ", "_").replace("?", "").replace("Count","my_count")
				coloumn.append(coloumn_second.replace("-", "_"))
				
		print coloumn
		
		
		
		str =""
		name = "my_table"
		
		
		table = filename[:-4].replace("-", "_")	
		print filename
		print table
		
		for c in coloumn:
				str =str+c+" "+"varchar(255)"+","
		print str
		str1 = str[:-1]
		print str1
		
		sql = "LOAD DATA LOCAL INFILE '"+filename+"' INTO TABLE "+table+" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES SET my_number=CEIL(RAND() *6) "
		print filename
		print table
		db = MySQLdb.connect(host="rakshadb.cdy91id5bbuk.us-west-2.rds.amazonaws.com",   
						user="raksha105",        
						passwd="myrakshadb",
						db="rakshadb")
						
		
		#print "create table "+table+"(Country varchar(255),station varchar(255),vmo_number int, unit varchar(255), jan float, feb float, mar float, apr float, may, float, jun float, jul float, aug float, sep float, oct float, nov float, dec float,ID int NOT NULL AUTO_INCREMENT,"+str1+",PRIMARY KEY (ID))"			
		
		cur = db.cursor()
		start_time = time.time()
		cur.execute("create table UNPrecip(Country varchar(255),station varchar(255),vmo_number int, unit varchar(255), jan float, feb float, mar float, apr float, may float, jun float, jul float, aug float, sep float, oct float, nov float, decem float,ID int NOT NULL AUTO_INCREMENT,my_number int, PRIMARY KEY (ID))")
		db.commit()
		cur.execute(sql)
		end_time = time.time() - start_time
		
		
		db.commit()
		del_time = time.time()
		cur.execute("delete from UNPrecip where (JAN > 10000 or FEB > 10000 or MAR > 10000)")
		db.commit()
		del_end_time = time.time() - del_time
		
		cur.execute("select COUNT(*) from UNPrecip")
		for r in cur.fetchall():
			num_count = r
		print r
		
		db.close()
		
		
	
		

		
		os.remove(filename)
		# return "this is your table name "+table+" with these coloumns "+str1+"""
		return render_template('dbtables.html',table = table, end_time = end_time,del_end_time=del_end_time)


@app.route('/query_normal', methods=['POST'])
def query_normal():

		connect_time = datetime.datetime.now()
		db = MySQLdb.connect(host="rakshadb.cdy91id5bbuk.us-west-2.rds.amazonaws.com",   
						user="raksha105",        
						passwd="myrakshadb",
						db="rakshadb")
		conn_done = datetime.datetime.now() - connect_time 
		
		tab_name = request.form.getlist('norm_table')
		#print tab_name[0]
		user_table = request.form.getlist('user_table_norm')
		u_query = user_table[0]
		
		
		# table_name = request.form['table']
		#cols = request.form['str1']
		
		# print "select MAX(MAR), MAX(JUN) from "+table_name
		# cur = db.cursor()
		# start_time = time.time()
		# cur.execute("select MAX(MAR), MAX(JUN), MIN(MAR), MIN(JUN) from "+table_name+" WHERE ((JUN) > 0) and(MAR) > 0")
		# end_time = time.time() - start_time
		# db.commit()
		# for row in cur.fetchall():
				# print row
		start_time = datetime.datetime.now()
		for i in range(1,10000):
				#ran_num = random.randint(1,6)
				cur = db.cursor()
				#print "select * from "+table_name+" where (ID BETWEEN 1 and 200) and my_number=FLOOR(RAND() *5);"
				
				cur.execute(u_query)
				
				rows = cur.fetchall()
				vals = rows
		end_time = datetime.datetime.now() - start_time				
						
		
		
		db.close()
		
		
		
		return "write query"+" "+str(end_time)+"connection time"+str(conn_done)

		
@app.route('/min_max_query', methods=['POST'])
def min_max_query():

		qu = request.form['do_min max']
		qu1 = request.form['min_max']
		
		return """ <form id ="user_input" enctype="multipart/form-data" method="post" action="/user_input">
		<input type ="text" name ="user_text"/>
		<input type ="submit" value ="submit query" name = "submit_query"></form>
"""
@app.route('/quest_7', methods=['POST'])
def quest_7():
		text_1 = request.form['u_i']
		tx = text_1
		text_2 = request.form['u_j']
		print tx
		print text_2
		# str_txt = str(tx)
		# arr_txt = str_txt.split(" ")
		# print type(arr_txt)
		# print arr_txt[2]
		
		# db = MySQLdb.connect(host="rakshadb.cdy91id5bbuk.us-west-2.rds.amazonaws.com",   
						# user="raksha105",        
						# passwd="myrakshadb",
						# db="rakshadb")
		# if(arr_txt[1] == 'E'):
			# month = 'jan'
		
		# if(arr_txt[1] == 'F'):
			# month = 'feb'
			
		# if(arr_txt[1] == 'G'):
			# month = 'mar'
			
		# if(arr_txt[1] == 'H'):
			# month = 'jun'
			
		# if(arr_txt[1] == 'H'):
			# month = 'jul'
			
		# if(arr_txt[1] == 'I'):
			# month = 'aug'
			
		# if(arr_txt[1] == 'J'):
			# month = 'sep'
			
		# if(arr_txt[1] == 'H'):
			# month = 'oct'
		
		# if(arr_txt[1] == 'I'):
			# month = 'nov'
			
		# if(arr_txt[1] == 'J'):
			# month = 'dec'
		
		# country = arr_txt[0]
		# symn = arr_txt[2]
		# val = arr_txt[3]
		
		# cur = db.cursor()				
		# start_time = datetime.datetime.now()
		# for i in range(1,200):
				# cur.execute("select * from UNPrecip where Country= '"+str(country)+"' and "+str(month)+" "+str(symn)+" "+str(val))
				# for row in cur.fetchall():
						# print row
				

		return "ascascasaas"
		


@app.route('/user_input', methods=['POST'])
def user_input():

		db = MySQLdb.connect(host="rakshadb.cdy91id5bbuk.us-west-2.rds.amazonaws.com",   
						user="raksha105",        
						passwd="myrakshadb",
						db="rakshadb")

		query_string = request.form['user_text']
		val = query_string.split()
		print val[1]
		
		if 'max' in query_string:
				cur = db.cursor()
				start_time1 = time.time()
				cur.execute("select COUNT(MAR), COUNT(JUN) from UNPrecip where (MAR <= "+val[1]+") and (JUN <= "+val[1]+") ")
				end_time1 = time.time() - start_time1
				db.commit()
				for row in cur.fetchall():
						print row
				return 'time taken is'+str(end_time1)
		if 'min' in query_string:
				cur = db.cursor()
				start_time2 = time.time()
				cur.execute("select COUNT(MAR), COUNT(JUN) from UNPrecip where Country ='ALEX and JUN > 10")
				end_time2 = time.time() - start_time2
				db.commit()
				for row in cur.fetchall():
						print row
				return 'time taken is'+str(end_time2)
		
		return 'invalid query'


@app.route('/memecache', methods=['POST'])
def memecache():
		
		
		
		db = MySQLdb.connect(host="rakshadb.cdy91id5bbuk.us-west-2.rds.amazonaws.com",   
						user="raksha105",        
						passwd="myrakshadb",
						db="rakshadb")
		
		
		
		
		
		
		mc = memcache.Client(['rakshacache.m2whgn.cfg.usw2.cache.amazonaws.com'], debug = 0)
		#mc.flush_all()
		
		#print str_query
		start_time = datetime.datetime.now()
		for i in range(1,200):
				#ran_num = random.randint(1,6)
				
				value = mc.get("rak")
				print value
				if value is None:
						
						print "fethcing from db"
						#print u_query
						cur = db.cursor()
						cur.execute("select * from UNPrecip where Country ='ALEX' and JUN > 10")
						rows = cur.fetchall()
						b =mc.set("rak",rows[0])
						print b
					# print "set to cache"
				
					# for row in value:
							# print "%s , %s" %(row[0],row[1])
				
				else:
						print "cache hit"
				
		end_time = datetime.datetime.now() - start_time
		db.commit()
		db.close()
		
		# print db_val
		
		return "done"+str(end_time)

@app.route('/c_content', methods=['POST'])
def c_content():

		mc = memcache.Client(['rakshacache.m2whgn.cfg.usw2.cache.amazonaws.com'], debug = 0)
		string_query = request.form['view_cache_contents']
		ss = hashlib.md5(string_query).hexdigest()
		print string_query
		value = mc.get("rak")

		if value is not None:
			
			print value
		else:
			print "not in cache"
		
		return "get cache cntent"


@app.route('/flush_cache', methods=['POST'])
def flush_cache():
		
		mc = memcache.Client(['rakshacache.m2whgn.cfg.usw2.cache.amazonaws.com'], debug = 0)

		mc.flush_all()
		return "flushed cache "

		
		
@app.route('/query_tuples', methods=['POST'])
def query_tuples():
		db = MySQLdb.connect(host="rakshadb.cdy91id5bbuk.us-west-2.rds.amazonaws.com",   
						user="raksha105",        
						passwd="myrakshadb",
						db="rakshadb")
		
		
		table_name = request.form['table_query']
		#cols = request.form['str1']
		
		for i in range(1,100):
				cur = db.cursor()
				#print "select * from "+table_name+" where ID="+str(i)+";"
				start_time = time.time()
				cur.execute("select * from "+table_name+" where my_number=FLOOR(RAND() *5);")
				end_time = time.time() - start_time
				db.commit()
				#for row in cur.fetchall():
						#print row
		
		db.commit()
		db.close()
		
		
		print end_time
		return "write query"+table_name+" "
		

		
@app.route('/upload_file', methods=['POST'])
def upload_file():
		file = request.files['myfile']
		filecontent=file.read()
		filename=file.filename
		
		fo = open(filename, "w")
		fo.write(filecontent);
		

		
		s3 = boto.connect_s3("AKIAICDVW6GK66SSXSHA","nCMhf2hcPH/OZq4FcqrNgaYDvowQPOfA2l0jRk/s")
		bucket = s3.create_bucket('orangeraksha') 
		start_time =time.time()
		key = bucket.new_key('allfiles/'+filename)
		key.set_contents_from_filename(filename)
		end_time = time.time() - start_time
		key.set_acl('public-read')
		
		
		fo.close()

		
		os.remove(filename)
		return 'your  file '  + ' is uploaded and it took '+str(end_time)
		

	
if __name__ == '__main__':
    
    app.run(debug = True)
	#app.run(host='0.0.0.0', port=port)