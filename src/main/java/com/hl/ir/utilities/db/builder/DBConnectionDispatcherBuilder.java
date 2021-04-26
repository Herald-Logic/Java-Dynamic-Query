package com.hl.ir.utilities.db.builder;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hl.ir.utilities.db.dispatcher.ConnectionType;
import com.hl.ir.utilities.db.dispatcher.DBConnectionDispatcher;
import com.hl.ir.utilities.properties.Client;
import com.hl.ir.utilities.properties.Properties;
import com.hl.ir.utilities.properties.exception.PropertyNotFoundException;
import com.hl.utilities.db.utils.DBConstants;

public class DBConnectionDispatcherBuilder {

	private static Logger logger = LoggerFactory.getLogger(DBConnectionDispatcherBuilder.class);
	private Client client;
	private DBConnectionDispatcher dbConnectionDispatcher;
	private ConnectionType connectionType ;
	public static Map<String, DBConnectionDispatcher> dbCache= new HashMap<String, DBConnectionDispatcher>();
	private static final Object LOCK = new Object();
	private Properties properties = null;
	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public static DBConnectionDispatcherBuilder builder() {
		return new DBConnectionDispatcherBuilder();
	}
	
	public DBConnectionDispatcher build() throws PropertyNotFoundException
	{
		try {
			//w.r.t Client Object load Properties from property manager
			client= Client.builder().fetch();
			properties = Properties.builder().client(client).build();
			connectionType= ConnectionType.valueOf(properties.getProperty(DBConstants.PROPERTY_KEY_DB_CONNECTION_TYPE));
			logger.debug("connectionType:{}", new Object[] {connectionType});
			dbConnectionDispatcher= getDBConnectionDispatcherObject(client.getClient());
			logger.debug("dbCache:{}", new Object[] {dbCache});
			return dbConnectionDispatcher;
		}
		catch (PropertyNotFoundException e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public DBConnectionDispatcher getDBConnectionDispatcherObject(String client) throws PropertyNotFoundException
	{
		if(dbCache.get(client) == null) {
			synchronized (LOCK) {
				if(dbCache.get(client) == null) {
						dbConnectionDispatcher = connectionType.getDBConnection(properties.getProperty(DBConstants.PROPERTY_KEY_DB_CONNECTION_TYPE),this);
						dbCache.put(client, dbConnectionDispatcher);
				}
			}
		}
		return dbCache.get(client);
	}
}
