package com.hl.utilities.db;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hl.utilities.db.exception.DBException;
import com.hl.utilities.db.utils.DBConstants;
import com.hl.utils.properties.PropertyManager;
import com.mchange.v2.c3p0.ComboPooledDataSource;

@Deprecated
public class PooledDBConnection extends DBConnection {

	private Logger logger = LoggerFactory.getLogger(PooledDBConnection.class);
	private ComboPooledDataSource dataSource;
	
	public PooledDBConnection(String endpointPrefix) {
		try {
			dataSource = new ComboPooledDataSource();
			dataSource.setDriverClass(PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DB_DRIVER));
			dataSource.setJdbcUrl(PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DB_URL));
			dataSource.setUser(PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DB_USERNAME));
			dataSource.setPassword(PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DB_PASSWORD));
		
			dataSource.setInitialPoolSize(Integer.parseInt(PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DBPOOL_INITIALSIZE, "0")));
			dataSource.setMinPoolSize(Integer.parseInt(PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DBPOOL_MINIMUMSIZE, "0")));
			dataSource.setMaxPoolSize((Integer.parseInt(PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DBPOOL_MAXSIZE, "0"))));
			dataSource.setMaxIdleTime(Integer.parseInt(PropertyManager.getParam(endpointPrefix + DBConstants.PROPERTY_KEY_DBPOOL_MAXIDLETIME, "0")));
		
		} catch (PropertyVetoException e){
        	logger.error("Unacceptable Driver Class", e);
        	throw new DBException(e.getMessage());
		}
	}
	
	@Override
	public Connection getConnection() throws SQLException {
		return this.dataSource.getConnection();
	}
	
	@Override
	public DataSource getDataSource() {
		return this.dataSource;
	}
	
	@Override
	public void shutdown() {
		int i = 1;
		try {
			while(this.dataSource.getThreadPoolNumActiveThreads() > 0 && i++ <= 5) {
				System.out.println("Waiting for Connections to close...");
				Thread.sleep(1000);
			}
			this.dataSource.close();
		} catch (SQLException e) {
			System.out.println("Error while shutting down datasource...");
		} catch (InterruptedException e) {
			System.out.println("Error while shutting down datasource...");
		}
	}

	@Override
	public Integer maxNoOfConnections() {
		try {
		return this.dataSource.getMaxPoolSize();
		}
		catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public Integer usedNoOfConnections() {
		Integer count;
		try {
			count =  this.dataSource.getNumBusyConnections();
		} catch (SQLException e) {
			e.printStackTrace();
			count = -1;
		}
		return count;
	}
}
