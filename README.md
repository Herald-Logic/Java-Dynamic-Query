# RDBMS Proxy

- Have properties file having the following properties

	## DB Configuration
	DB.TYPE=POSTGRES
	#DB.CONNECTION.TYPE=SIMPLEDBCONNECTION/POOLEDDBCONNECTION
	DB.CONNECTION.TYPE=POOLEDDBCONNECTION
	DB.DRIVER=org.postgresql.Driver
	DB.URL=jdbc:postgresql://cluster-hdfc-nonprd-1.cluster-cjeafljzaii3.ap-south-1.rds.amazonaws.com:5434/HDFC_DEV
	DB.USERNAME=#####
	DB.PASSWORD=####
	DB.POOL.INITIAL.SIZE=5
	DB.POOL.MINIMUM.SIZE=1
	DB.POOL.MAX.SIZE=100
	DB.POOL.MAX.IDLE.TIME=10

- To load the properties file use the following code
	- PropertyManager.loadProperties(...) - Overloaded method, use any of the appropriate method. If only file name passed, it will look the file name in classpath.

- The following code will be used to get db connections and configurations
	- DBManager.getInstance().get...
	- 'Endpoint Name' is used when we want to have connections to multiple DBs. The configurations for multiple DBs would be same as above append prefix.
		- Example : READER.DB.TYPE=POSTGRES
		- Here, 'READER' is the endpointName. All the configurations need to have the same prefix to make it related.

- StatementFactory.createStatement(...) will provide you with Statement Object. Database Type is automatically selected from the DB Configurations.
- Building the statement object is self explanatory through method names.
- To print a query formed by the Statement
	- System.out.println(statement)
- To execute the query in the Database, use StatementUtil class.