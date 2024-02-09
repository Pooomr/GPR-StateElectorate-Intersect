'''	Update GPR Lot-Electorate records
		This is will update the S_LOT_ELECTORATE table in GPR Database by inserting Lot->Electorate records that do not exist in the database yet.
		
		1. Get list of current GPR Lots that do not have a match in S_ELECTORATE table AND also have a CADID (This indicates that the lot has spatial information)
		2. Get Spatial geometry data from SIX Maps REST Service and create GPR_LOT gdb
		3. Get Spatial geometry of Electorates from SIX Maps and create gdb
		4. Compare Electorate names with GPR Electorate names to ensure GPR Electorates are up to date
		5. Intersect GPR_LOT gdb with Electorate gdb (using tolerance of 10cm)
		6. Insert intersection results to S_LOT_ELECTORATE table
	
'''

import logging
import sys
#logging.basicConfig(level=logging.DEBUG)
username = sys.argv[1]
logging.basicConfig(filename="log.txt",
					level=logging.INFO,
					format="%(asctime)s - {} - %(message)s".format(username),
					datefmt='%d/%m/%Y %H:%M:%S')
logging.debug("Importing Python Packages...")
logging.info("[START] GPR Electorate Update process started")

try:
	import arcpy
except:
	print("Error Importing arcpy module, make sure OpenVPN is connected!")
	logging.error("[STOPPED] Unable to import arcpy module, GPR electorate update Stopped")
	sys.exit()

import cx_Oracle
import requests
import json
import pandas as pd
import config
import os

logging.debug("Python packages imported successfully")

def loadingBar(p: int, msg: str) -> str:
	
	progress = ""
	togo = "          "
	
	togo = togo[:-p] #reduce empty space based on progress
	
	for i in range(p):
		progress += "■"
		
	
	print("[{}{}] {}                            ".format(progress, togo, msg), end="\r")

def getNextId(column: str, table: str) -> int:
	c.execute("select max({}) from {}".format(column, table))
	result = c.fetchone()
	
	#If records exist, increment next id, else start at 1
	if result[0] != None:
		nextId = result[0] + 1
	else:
		nextId = 1
	
	return nextId

def connectDB():
	#Connects to GPR Database
	connection = None

	oc_attempts = 0

	while oc_attempts < 2:
		if oc_attempts == 0:
			print("Trying DPE IP: {}".format(config.dsnDPE))
			dsn = config.dsnDPE
		else:
			dsn = config.dsnDCS
			print("Trying DCS IP: {}".format(config.dsnDCS))
			
		try:
			connection = cx_Oracle.connect(
				config.username,
				config.password,
				dsn,
				encoding=config.encoding)

			# show the version of the Oracle Database
			print(connection.version," Connection Successful!")
			oc_attempts = 2
		except cx_Oracle.Error as error:
			print(error)
			oc_attempts += 1
			
	return connection

def getRESTData(baseURL, params, serviceName):
	
	retries = 0
	success = False
	while not success:
		try:
			response = requests.get(url=baseURL, params=params)
			success = True
		except requests.exceptions.RequestException as e:
			print(e)
			retries += 1
			if retries > 9:
				while True:
					select = input("\nRequest to {} service failed 10 times, Do you want to try again? y/n\n".format(serviceName))
					if select == "y":
						retries = 0
						break
					elif select == "n":
						print("GPR Electorate update process Aborted!!")
						sys.exit()
					else:
						print("Invalid selection. Please enter y or n")
		
		while response.status_code != 200 and success:
			print("Response code: {}".format(response.status_code))
			select2 = input("\nInvalid response received, run query again? y/n\n")
			if select2 == "y":
				retries = 0
				success = False
				break
			elif select2 == "n":
				print("GPR Electorate update process Aborted!!")
				sys.exit()
			else:
				print("Invalid selection. Please enter y or n")
	
	return json.loads(response.text)
	
def writeToJSON(JSONHead, tempJSON, JSONResults, layerName):
	
	JSONinput = ""
	JSONinput += "{}".format(JSONHead)
	totalSLots = len(JSONResults)
	fileNum = 1
	
	logging.debug("WRITING TO JSON... {}".format(totalSLots))
	for i, row in enumerate(JSONResults):
		
		#Add lot records to JSON
		if (i + 1) % 3000 == 1:
			JSONinput += '{}'.format(JSONResults[i]) #If first record do not add comma
		else:
			JSONinput += ',{}'.format(JSONResults[i])
		
		#If max range met, close file and open new one
		if (i + 1) % 3000 == 0 or (i + 1) == totalSLots:
			JSONinput += ']}'
			logging.debug("Writing to JSON file at {}".format(tempJSON))
			#Clear Temp JSON file and insert results
			with open(tempJSON,'w') as jsonDir:
				jsonDir.write(JSONinput.replace("None","null")) #Replace instances of 'None' with null
			
			logging.debug("Writing to scratch arcGIS project folder...")
			#Load to arcGIS folder
			arcpy.conversion.JSONToFeatures(tempJSON,"{}\\arcGIS\\scratch.gdb\\{}_{}".format(os.getcwd(),layerName, fileNum),"POLYGON")
			
			fileNum += 1
			JSONinput = "{}".format(JSONHead) #Reset
			
	#Merge all scratch files
	LayerList = ''
	logging.debug("Merge {} files".format(fileNum - 1))
	for layer in range(1, fileNum):
		logging.debug("processing {} of {}".format(layer, fileNum - 1))
		if layer == 1:
			LayerList += "{}\\arcGIS\\scratch.gdb\\{}_{}".format(os.getcwd(),layerName,layer)
		else:
			LayerList += ";{}\\arcGIS\\scratch.gdb\\{}_{}".format(os.getcwd(),layerName,layer)
	logging.debug("Run: Merge({},{})".format(LayerList,"{}\\arcGIS\\gpr_state_electorate.gdb\\{}".format(os.getcwd(),layerName)))	
	arcpy.management.Merge(LayerList, "{}\\arcGIS\\gpr_state_electorate.gdb\\{}".format(os.getcwd(),layerName))
	
def updateStateElectorate():
	#Update/Create State Electorate GDB
	
	#Paramaters
	dir = "{}\\arcGIS\\gpr_state_electorate.gdb".format(os.getcwd())
	tempJSON = "{}\\arcGIS\\Temp.json".format(os.getcwd())
	baseURL = "https://maps.six.nsw.gov.au/arcgis/rest/services/sixmaps/Boundaries/MapServer/3/query"
	seIDname = ''
	JSONHead = '{"displayFieldName":"districtname","fieldAliases":{"rid":"RID","cadid":"cadid","createdate":"createdate","modifieddate":"modifieddate","districtname":"districtname","startdate":"startdate","enddate":"enddate","lastupdate":"lastupdate","msoid":"msoid","centroidid":"centroidid","shapeuuid":"shapeuuid","changetype":"changetype","processstate":"processstate","urbanity":"urbanity","shape_Length":"shape_Length","shape_Area":"shape_Area"},"geometryType":"esriGeometryPolygon","spatialReference":{"wkid":4326,"latestWkid":4326},"fields":[{"name":"rid","type":"esriFieldTypeOID","alias":"RID"},{"name":"cadid","type":"esriFieldTypeInteger","alias":"cadid"},{"name":"createdate","type":"esriFieldTypeDate","alias":"createdate","length":8},{"name":"modifieddate","type":"esriFieldTypeDate","alias":"modifieddate","length":8},{"name":"districtname","type":"esriFieldTypeString","alias":"districtname","length":60},{"name":"startdate","type":"esriFieldTypeDate","alias":"startdate","length":8},{"name":"enddate","type":"esriFieldTypeDate","alias":"enddate","length":8},{"name":"lastupdate","type":"esriFieldTypeDate","alias":"lastupdate","length":8},{"name":"msoid","type":"esriFieldTypeInteger","alias":"msoid"},{"name":"centroidid","type":"esriFieldTypeInteger","alias":"centroidid"},{"name":"shapeuuid","type":"esriFieldTypeString","alias":"shapeuuid","length":38},{"name":"changetype","type":"esriFieldTypeString","alias":"changetype","length":2},{"name":"processstate","type":"esriFieldTypeString","alias":"processstate","length":5},{"name":"urbanity","type":"esriFieldTypeString","alias":"urbanity","length":2},{"name":"shape_Length","type":"esriFieldTypeDouble","alias":"shape_Length"},{"name":"shape_Area","type":"esriFieldTypeDouble","alias":"shape_Area"}],"features":['
	
	seIDResults = list()
	
	#Get Object ids
	params = {
				'f':'json',
				'returnGeometry':'false',
				'returnIdsOnly':'true',
				'where':"1=1"
	}
				
	jsonResult = getRESTData(baseURL, params, "State Electorate")
	
	if jsonResult.get('objectIds'):
		
		seIDname = jsonResult['objectIdFieldName']
		
		#iterate through all features in JSON response and add to Result list
		for jr in range(len(jsonResult['objectIds'])):
			seIDResults.append(jsonResult['objectIds'][jr])
	
	jnum = 1 #Track number of JSON files
	seIDs = ''
	sElectorates = list()
	
	#Get State Electorate Geometries
	for i, row in enumerate(seIDResults):
		if seIDs == '':
			seIDs += '{}'.format(row)
		else:
			seIDs += ',{}'.format(row)
		
		if (i + 1) % 50 == 0 or (i + 1) == len(seIDResults) :
			#Every 50 records, save to JSON and import into arcGIS gdb
			params = {
				'f':'json',
				'returnGeometry':'true',
				'outSR':'4326',
				'OutFields':'*',
				'where':'{} in ({})'.format(seIDname,seIDs)
			}
			logging.debug("Params are {}, {}".format(seIDname, seIDs))
						
			jsonResult = getRESTData(baseURL, params, "State Electorate")
			
			if jsonResult.get('features'):
				#iterate through all features in JSON response and add to Result list
				for jr in range(len(jsonResult['features'])):
					sElectorates.append(jsonResult['features'][jr])
					
			#Reset State Electorate ID list
			seIDs = ''
	
	#Save results to temp JSON and load to ArcGIS
	writeToJSON(JSONHead, tempJSON, sElectorates, 'state_electorate')	
					
if __name__ == "__main__":
	
	logging.debug("Starting GPR Electorate update process...")
	
	#ArcPy Settings
	arcpy.env.overwriteOutput = True
	arcFolder = "{}\\arcGIS\\gpr_state_electorate.gdb".format(os.getcwd())
	arcpy.env.workspace = arcFolder
	
	#Set table names
	au_lot = "au_electorate_gpr_lots"
	
	#Lot cadastre url
	baseURL = "https://maps.six.nsw.gov.au/arcgis/rest/services/sixmaps/Cadastre/MapServer/0/query"
	
	#State Electorate url
	baseSEURL = "https://maps.six.nsw.gov.au/arcgis/rest/services/sixmaps/Boundaries/MapServer/3/query"
	
	#Temporary JSON file
	tempJSON = "{}\\arcGIS\\Temp.json".format(os.getcwd())
	
	#JSON Head for Lots
	JSONHead = '{"displayFieldName": "planlabel","fieldAliases": {"lotidstring":"lotidstring" },"geometryType": "esriGeometryPolygon","spatialReference": {"wkid": 4326,"latestWkid": 4326},"fields": [{"name":"lotidstring","type":"esriFieldTypeString","alias":"lotidstring","length":50} ],"features": ['

		
	#connect to GPR DB
	connection = connectDB()
	c = connection.cursor()
	
	loadingBar(1,"10% - Checking State Electorate Data...")
	
	#Check State electorate table in GPR is up to date
	logging.debug("Checking GPR State electorate data...")
	params = {
				'f':'json',
				'returnGeometry':'false',
				'OutFields':'districtname',
				'where':'1=1'
	}
	jsonResult = getRESTData(baseSEURL, params, "State Electorate")
	df_electorates = pd.json_normalize(jsonResult["features"])
	df_electorates = df_electorates.rename(columns={'attributes.districtname': 'DISTRICTNAME'})
	
	df_gpr_electorates = pd.read_sql("select distinct upper(district_name) districtname from s_electorate where end_date is null",connection)
	
	df_electorate_diff = df_electorates.merge(df_gpr_electorates, indicator=True, how='outer').query('_merge != "both"').drop('_merge', 1) #Return differences between Current State electorate and GPR data
	
	if len(df_electorate_diff) > 0:
		print("-------------------------------------------")
		print(" GPR State Electorate data does not match!")
		print("-------------------------------------------")
		for i, electorate in df_electorate_diff.iterrows():
			print(electorate["DISTRICTNAME"])
		exit()
	else:
		logging.debug("GPR State Electorate data is up to date!")
	
	tables = [au_lot]
	
	loadingBar(2,"20% - Dropping and creating new table...")
	
	for table in tables:
		#Check if table exists
		query = "select * from all_tables where table_name = UPPER('{}')".format(table)
		c.execute(query)
		result = c.fetchone()
		logging.debug("Checking if table {} exists...".format(table))

		if result:
			logging.debug("Table {} exists".format(table))
			query = "drop table {}".format(table)
			c.execute(query)
			logging.debug("{} dropped successfully".format(table))
		else:
			logging.debug("Table {} doesn't exist".format(table))
	
	#Create table of lots to find electorate
	c.execute("create table {} as \
				select l.lot_no || '/' || l.section_no || '/' || l.plan_type || l.plan_no lotref,\
						case\
						when l.plan_type = 'DP'\
							then l.lot_no\
						end || '/' || l.section_no || '/' || l.plan_type || l.plan_no s_lotref,\
						l.lot_id, l.lot_cad_id\
				from lot l\
				where l.lot_cad_id is not null\
				and l.end_date is null\
				and not exists (select * from s_lot_electorate sle\
								where l.lot_id = sle.lot_id)\
				and l.create_date > (SYSDATE - 90)".format(au_lot))
				
	logging.debug(f"{au_lot} created succesfully!")
	
	#Create list of unique S_Lotref to query Lot service
	df_s_lots = pd.read_sql("select distinct s_lotref from {}".format(au_lot),connection)
	
	#If no lots require update, exit script
	if len(df_s_lots) == 0:
		print("-------------------------------------------------")
		print(" No lots to update. Exiting electorate update...")
		print("-------------------------------------------------")
		exit()
	
	#initialise string to pass through Lot Cadastre query
	lotstring = ''
	
	#Initialise List to store all Json results
	lotResults = list()
	
	loadingBar(3,"30% - Querying Lot Service...")
	
	for i, row in df_s_lots.iterrows():
		if lotstring == '':
			lotstring += "'{}'".format(row["S_LOTREF"])
		else:
			lotstring += ",'{}'".format(row["S_LOTREF"])
		
		#Every 200 records query service
		if (i + 1) % 200 == 0 or (i + 1) == len(df_s_lots):
			
			params = {
				'f':'json',
				'returnGeometry':'true',
				'outSR':'4326',
				'OutFields':'lotidstring',
				'where':'lotidstring in ({})'.format(lotstring)
			}
						
			jsonResult = getRESTData(baseURL, params, "Lot Service")
					
			if jsonResult.get('features'):
				#iterate through all features in JSON response and add to Result list
				for jr in range(len(jsonResult['features'])):
					lotResults.append(jsonResult['features'][jr])
				
			lotstring = ''
	
	#if no spatial lots returned, exit script
	if len(lotResults) == 0:
		print("---------------------------------------------------------")
		print(" No lots with spatial data. Exiting electorate update...")
		print("---------------------------------------------------------")
		exit()
	
	loadingBar(4,"40% - Checking arcGIS folders...")
	
	#If Scratch folder doesn't exist, create it
	if not os.path.exists("{}\\arcGIS\\scratch.gdb".format(os.getcwd())):
		arcpy.management.CreateFileGDB("{}\\arcGIS".format(os.getcwd()), "scratch.gdb")
		logging.debug("Scratch folder created...")
	
	#Check if State Electorate file exists
	if not os.path.exists("{}\\arcGIS\\gpr_state_electorate.gdb".format(os.getcwd())):
		arcpy.management.CreateFileGDB("{}\\arcGIS".format(os.getcwd()), "gpr_state_electorate.gdb")
		logging.debug("No gpr_state_electorate.gdb found, create new file...")
		
		updateStateElectorate()
		
		logging.debug("{}\\arcGIS\\gpr_state_electorate.gdb did not exist, gdb created successfully!".format(os.getcwd()))
	
	loadingBar(4,"45% - Checking State Electorate layer...")
	
	#Check if State Electorate file needs to be updated
	table = "{}\state_electorate".format(arcFolder)
	columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"]
	df_se = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns) #Store State electorate data into dataframe
	
	
	df_se['lastupdate'] = pd.to_datetime(df_se['lastupdate']) 
	latest_se = df_se['lastupdate'].max() + pd.to_timedelta(1,unit='s') #Get latest modified date add 1 second to round up due to milliseconds
	#latest_se = df_se['lastupdate'].max() #Get latest modified date

	#Get latest modified date from State Electorate service
	params = {
				'f':'json',
				'returnGeometry':'false',
				'returnCountOnly':'true',
				'where':"lastupdate > TIMESTAMP '{}'".format(latest_se.strftime('%Y-%m-%d %H:%M:%S'))
	}
	
	jsonResult = getRESTData(baseSEURL, params, "State Electorate")
	
	#If there are records updated after latestupdate value in current layer, refresh State electorate layer
	if jsonResult['count'] > 0:
		logging.debug("{} State electorates have been updated, refreshing layer...".format(jsonResult['count']))
		updateStateElectorate()
	
	loadingBar(5,"50% - Importing Lots to arcGIS gdb...")
	
	#Create JSON files for ArcGIS processing
	logging.debug("Importing Lots to ArcGIS folder...")
	writeToJSON(JSONHead, tempJSON, lotResults, 'gpr_lots_to_elec')
	
	loadingBar(6,"60% - Intersecting Lot and State Electorates...")
	
	#Intersect Lot with Electorate layer
	logging.debug("Intersecting GPR Lots with State Electorates...")
	arcpy.analysis.TabulateIntersection("{}\\gpr_lots_to_elec".format(arcFolder), "lotidstring", "{}\\state_electorate".format(arcFolder), "{}\\gpr_lots_electorate".format(arcFolder), "cadid;districtname", None, "10 Centimeters", "SQUARE_METERS")
	
	loadingBar(7,"70% - Transforming results...")
	
	#Store intersection results into dataframe
	table = "{}\\gpr_lots_electorate".format(arcFolder)
	columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"]
	df_le = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
	df_le = df_le.rename(columns={'districtname': 'DISTRICTNAME'})
	
	loadingBar(8,"80% - Transforming results...")
	
	#Get GPR State Electorate CADIDs (Electorate cadid accuracy is not important, only the electorate name is used for GPR reporting, use GPR's cadids to avoid potential conflicts)
	df_gpr_e_cad = pd.read_sql("select distinct upper(district_name) districtname, electorate_cadid from s_electorate where end_date is null",connection)
	df_cad = pd.merge(df_le, df_gpr_e_cad)
	
	#Get GPR Lot details
	df_gpr_lot = pd.read_sql("select * from au_electorate_gpr_lots",connection)
	df_gpr_lot = df_gpr_lot.rename(columns={'S_LOTREF': 'lotidstring'})
	
	loadingBar(8,"85% - Transforming results...")
	
	#Merge with Intersection results
	df_upload = pd.merge(df_cad, df_gpr_lot)
	
	loadingBar(9,"90% - Updating Table...")
	
	#Insert results into S_LOT_ELECTORATE table
	nextID = getNextId("ID","S_LOT_ELECTORATE")
	prevID = nextID - 1 #Keep track 
	for i, row in df_upload.iterrows():
		#print("insert into s_lot_electorate (id,electorate_cadid,lot_cadid,lot_id,lot_reference,sync_run_time) values ({},{},{},{},'{}',CURRENT_TIMESTAMP)".format(nextID,row['ELECTORATE_CADID'],row['LOT_CAD_ID'],row['LOT_ID'],row['LOTREF']))
		c.execute("insert into s_lot_electorate (id,electorate_cadid,lot_cadid,lot_id,lot_reference,sync_run_time) values ({},{},{},{},'{}',CURRENT_TIMESTAMP)".format(nextID,row['ELECTORATE_CADID'],row['LOT_CAD_ID'],row['LOT_ID'],row['LOTREF']))
		nextID += 1
	
	c.execute("commit")
	
	print("---------------------------------   ")
	print(" GPR Electorate Update complete!")
	print("---------------------------------")
	print("  Total records updated: {}\n".format(len(df_upload)))
	
	df_results = pd.read_sql("select count(*) total, UPPER(se.district_name) DISTRICTNAME from s_lot_electorate sle, s_electorate se where id > {} and sle.electorate_cadid = se.electorate_cadid group by se.district_name order by count(*) desc".format(prevID),connection)
	
	for i, row in df_results.iterrows():
		print("   {} x {}".format(row['TOTAL'],row['DISTRICTNAME']))
	
	print("---------------------------------")
	connection.close()
	
	logging.info("[FINISH] GPR Electorate Update Completed")
	