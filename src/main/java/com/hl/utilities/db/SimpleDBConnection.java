package com.hl.utilities.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.hl.utilities.db.utils.DBConstants;
import com.hl.utils.properties.PropertyManager;

@Deprecated
public class SimpleDBConnection extends DBConnection {

	private String dbUrl;
	private String username;
	private String password;
	
	public SimpleDBConnection(String endpointPrefix) throws ClassNotFoundException {
		Class.forName(PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DB_DRIVER));
		this.dbUrl = PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DB_URL);
		this.username = PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DB_USERNAME);
		this.password = PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DB_PASSWORD);
	}
	
	@Override
	public Connection getConnection() throws SQLException {
		return DriverManager.getConnection(dbUrl, username, password);
	}

	@Override
	public void shutdown() {}
	
	@Override
	public Integer maxNoOfConnections() {
		return 1;
	}

	@Override
	public Integer usedNoOfConnections() {
		return 1;
	}
}
