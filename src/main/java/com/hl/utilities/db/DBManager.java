package com.hl.utilities.db;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;	
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;
import javax.sql.rowset.CachedRowSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hl.utilities.db.exception.DBException;
import com.hl.utilities.db.utils.DBConstants;
import com.hl.utils.properties.PropertyManager;
import com.hl.utils.properties.exception.PropertyFileNotFoundException;
import com.sun.rowset.CachedRowSetImpl;

@Deprecated
public class DBManager {

	private static Logger logger = LoggerFactory.getLogger(DBManager.class);
	
	
	private static DBManager dbManager;
	private boolean initializeDefaultProperties;
	
	private Map<String, DBConnection> dbConnectionMap;
	private String defaultEndpoint = "";
	
	private DBManager(boolean initializeDefaultProperties) throws IOException, PropertyFileNotFoundException {
		if(initializeDefaultProperties && !PropertyManager.isPropertiesLoaded())
			PropertyManager.loadProperties("db.properties", true);
		
		if(dbConnectionMap == null)
			dbConnectionMap = new HashMap<String, DBConnection>();
		
		this.initializeDefaultProperties = initializeDefaultProperties;
		this.defaultEndpoint = PropertyManager.getString(DBConstants.PROPERTY_KEY_DB_DEFAULT, DBConstants.DB_ENDPOINT_DEFAULT);
	}

	/**
	 * Gives Singleton Instance of DBManager.
	 * Property 'DB.CONNECTION.TYPE' is referred by default for the connection type if initializing for the first time. 
	 */
	public static DBManager getInstance() {
		return getInstance(true);
	}
	
	/**
	 * Gives Singleton Instance of DBManager.
	 * Property 'DB.CONNECTION.TYPE' is referred by default for the connection type if initializing for the first time.
	 * @param initializeDefaultProperties If connection need not be initialized by default.
	 * @return
	 */
	/*public static DBManager getInstance(boolean initializeDefaultProperties) {
		return getInstance(initializeDefaultProperties);
	}*/
	
	/**
	 * Gives Singleton Instance of DBManager.
	 * 
	 * @param initializeDefaultProperties
	 * @return
	 */
	public static DBManager getInstance(boolean initializeDefaultProperties) {
		//Reinitialize DB Manager only if defaultProperties were not initialized previously and asked to be initialized now.
		if(dbManager == null || (!dbManager.initializeDefaultProperties && initializeDefaultProperties)) {
			synchronized (DBManager.class) {
				if(dbManager == null || (!dbManager.initializeDefaultProperties && initializeDefaultProperties)) {
					try {
						closePreviousConnections();
						dbManager = new DBManager(initializeDefaultProperties);
					}
					catch (Throwable e) {
						logger.error("Failed to Initialize DBManager",e);
						throw new InstantiationError(e.getMessage());
					}
				}
			}
		}
		return dbManager;
	}
	
	private static void closePreviousConnections() {
		if(dbManager != null && dbManager.dbConnectionMap != null) {
			for (Entry<String, DBConnection> dbConnectionEntry : dbManager.dbConnectionMap.entrySet()) {
				if(dbConnectionEntry.getValue() != null)
					dbConnectionEntry.getValue().shutdown();
			}
		}
	}
	
	private void initializeDBConnection(String endpointName) {
		
		logger.debug("Endpoint Name : {}", endpointName);
		if(endpointName == null)
			throw new DBException("Invalid Endpoint: " + endpointName);
		//Return if endpoint already initialized
		if(this.dbConnectionMap.containsKey(endpointName))
			return;
		
		//Initializing endpoint prefix
		String endpointPrefix = endpointName.equals(DBConstants.DB_ENDPOINT_DEFAULT) || endpointName.isEmpty() 
									? "" : (endpointName + DBConstants.DB_ENDPOINT_SEPERATOR);
		DBConnection dbConnection = null;
		
		logger.debug("Endpoint Prefix : {}", endpointPrefix);
		try {
			//Check whether Endpoint Conifguration exists
			String dbTypeValue = PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DB_TYPE, null);
			if(dbTypeValue == null) {
				logger.debug("Endpoint {} not configured.", endpointName);
				//Check if default endpoint is not configured
				if(this.defaultEndpoint.equalsIgnoreCase(endpointName)) {
					logger.error("Default Endpoint {} not configured.", endpointName);
					throw new DBException("Default Endpoint " + this.defaultEndpoint + " not configured.");
				}
				//If endpoint passed is not configured, then use default endpoint and add the endpoint to map pointing it to default
				System.out.println("Endpoint " + endpointName + " not configured. Hence, returning default endpoint : " + this.defaultEndpoint);
				logger.debug("Endpoint {} not configured. Hence, returning default endpoint : {}" , endpointName, this.defaultEndpoint);
				initializeDBConnection(this.defaultEndpoint);
				dbConnectionMap.put(endpointName, dbConnectionMap.get(this.defaultEndpoint));
				return;
			}
			
			ConnectionType connectionType = ConnectionType.valueOf(PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DB_CONNECTION_TYPE));
			DBType dbType = DBType.valueOf(dbTypeValue);
			
			dbConnection = connectionType.getDBConnection(endpointPrefix);
			dbConnection.setConnectionType(connectionType).setDbType(dbType);
			
			logger.debug("Adding DBConnection {} of endpoint {} to DBConnectionMap" , dbConnection, endpointName);
			this.dbConnectionMap.put(endpointName, dbConnection);
		}
		catch (IllegalArgumentException e) {
			throw new DBException("Invalid DB Type or DB Connection Type for endpointPrefix : " + endpointPrefix);
		} 
	}
	
	private DBConnection getDBConnection(String endpointName) {
		endpointName = endpointName == null ? this.defaultEndpoint : endpointName;
		initializeDBConnection(endpointName);
		logger.debug("Endpoint {}, DBConnection {}", endpointName, dbConnectionMap.get(endpointName));
		return dbConnectionMap.get(endpointName);
	}

	public Connection getConnection() throws SQLException {
		return getConnection(this.defaultEndpoint);
	}
	public Connection getConnection(String endpointName) throws SQLException {
		return getDBConnection(endpointName).getConnection();
    }
	
	public DataSource getDataSource() {
		return getDataSource(this.defaultEndpoint);
	}
	public DataSource getDataSource(String endpointName) {
		return getDBConnection(endpointName).getDataSource();
	}
	
	public String getConnectionType() {
		return getConnectionType(this.defaultEndpoint);
	}
	public String getConnectionType(String endpointName) {
		return getDBConnection(endpointName).getConnectionType().name();
	}
	
	public DBType getDbType() {
		return getDbType(this.defaultEndpoint);
	}
	public DBType getDbType(String endpointName) {
		return getDBConnection(endpointName).getDbType();
	}
	
	public int getMaxConnections() {
		return getDBConnection(this.defaultEndpoint).maxNoOfConnections();
	}
	public int getMaxConnections(String endpointName) {
		return getDBConnection(endpointName).maxNoOfConnections();
	}
	
	public int getUsedConnections() {
		return getDBConnection(this.defaultEndpoint).usedNoOfConnections();
	}
	public int getUsedConnections(String endpointName) {
		return getDBConnection(endpointName).usedNoOfConnections();
	}
	
	// Query Methods
	public CachedRowSet select(Connection connection, String query) throws SQLException{
		return select(connection, query, null);
	}
	
	private CachedRowSet select(Connection connection, String query, PreparedStatement pst) throws SQLException{
		boolean returnConnection = false;
		boolean returnPreparedStatement = false;
		CachedRowSet rowSet =  new CachedRowSetImpl();
		try {
			if(connection == null){
				connection = getConnection();
				returnConnection = true;
			}
			if(pst == null){
				pst = connection.prepareStatement(query.toString());
				returnPreparedStatement = true;
			}
			rowSet.populate(pst.executeQuery());
		} 
		finally {
			try {
				if (returnPreparedStatement && pst != null) {
					pst.close();
				}
				if(returnConnection && connection != null){
					connection.close();
				}
			} catch (Exception e) {}
		}
		return rowSet;
	}
}