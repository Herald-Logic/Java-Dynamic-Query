package com.hl.ir.utilities.db.dispatcher;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.dbcp2.PoolingDriver;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.hl.ir.utilities.PoolConnectionFactory;
import com.hl.ir.utilities.db.builder.DBConnectionDispatcherBuilder;
import com.hl.ir.utilities.properties.Client;
import com.hl.ir.utilities.properties.Properties;
import com.hl.ir.utilities.properties.builder.Type;
import com.hl.ir.utilities.properties.exception.PropertyNotFoundException;
import com.hl.utilities.db.utils.DBConstants;

public class PooledDBConnection implements DBConnectionDispatcher{

	private String dbUrl;
	private String username;
	private String password;
	private Properties properties;
	public PooledDBConnection() {}
	private DataSource dataSource;
  
	public PooledDBConnection(DBConnectionDispatcherBuilder builder) throws ClassNotFoundException, PropertyNotFoundException{
		
		properties = builder.getProperties();
		Class.forName(properties.getProperty(DBConstants.PROPERTY_KEY_DB_DRIVER));
		this.dbUrl = properties.getProperty(DBConstants.PROPERTY_KEY_DB_URL);
		this.username = properties.getProperty(DBConstants.PROPERTY_KEY_DB_USERNAME);
		this.password = properties.getProperty(DBConstants.PROPERTY_KEY_DB_PASSWORD);
		initializeConfiguration();
	}
	
	public void initializeConfiguration() throws NumberFormatException, PropertyNotFoundException {

    	java.util.Properties javaProperties = new java.util.Properties();
    	javaProperties .setProperty("user", username);
    	javaProperties .setProperty("password", password);
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(dbUrl,javaProperties);
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        
        config.setMaxTotal(Integer.parseInt(properties.getProperty(DBConstants.PROPERTY_KEY_DBPOOL_MAXSIZE)));
        config.setMaxIdle(Integer.parseInt(properties.getProperty(DBConstants.PROPERTY_KEY_DBPOOL_MAXIDLETIME)));
        config.setMinIdle(Integer.parseInt(properties.getProperty(DBConstants.PROPERTY_KEY_DBPOOL_MINIMUMSIZE)));
        ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory, config);
        poolableConnectionFactory.setPool(connectionPool);
        dataSource = new PoolingDataSource<>(connectionPool);
	}
	
	@Override
	public Connection getConnection() throws SQLException {
		System.out.println("pooled db connection");
		return dataSource.getConnection();
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
