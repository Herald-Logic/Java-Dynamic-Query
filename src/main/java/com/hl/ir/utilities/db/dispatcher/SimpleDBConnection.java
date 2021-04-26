package com.hl.ir.utilities.db.dispatcher;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.hl.ir.utilities.db.builder.DBConnectionDispatcherBuilder;
import com.hl.ir.utilities.properties.Client;
import com.hl.ir.utilities.properties.Properties;
import com.hl.ir.utilities.properties.builder.Type;
import com.hl.ir.utilities.properties.exception.PropertyNotFoundException;
import com.hl.utilities.db.utils.DBConstants;

public class SimpleDBConnection implements DBConnectionDispatcher{

	private String dbUrl;
	private String username;
	private String password;
	private Properties properties;
	public SimpleDBConnection() {}
	public SimpleDBConnection(DBConnectionDispatcherBuilder builder) throws ClassNotFoundException, PropertyNotFoundException {
		
		properties = builder.getProperties();
		Class.forName(properties.getProperty(DBConstants.PROPERTY_KEY_DB_DRIVER));
		this.dbUrl = properties.getProperty(DBConstants.PROPERTY_KEY_DB_URL);
		this.username = properties.getProperty(DBConstants.PROPERTY_KEY_DB_USERNAME);
		this.password = properties.getProperty(DBConstants.PROPERTY_KEY_DB_PASSWORD);
	}
	
	@Override
	public Connection getConnection() throws SQLException {
		return  DriverManager.getConnection(dbUrl, username, password);
	}
	
	public String getDbUrl() {
		return dbUrl;
	}
	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
}
