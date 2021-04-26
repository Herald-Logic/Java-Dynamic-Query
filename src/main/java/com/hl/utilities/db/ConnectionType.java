package com.hl.utilities.db;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.hl.utilities.db.exception.DBException;

@Deprecated
public enum ConnectionType {
	SIMPLEDBCONNECTION(SimpleDBConnection.class),
	POOLEDDBCONNECTION(PooledDBConnection.class);
	
	Class<? extends DBConnection> dbConnectionClazz;
	
	private ConnectionType(Class<? extends DBConnection> dbConnectionClazz) {
		this.dbConnectionClazz = dbConnectionClazz;
	}
	
	public DBConnection getDBConnection(String endpointPrefix) {
		try {
			Constructor<? extends DBConnection> constructor = this.dbConnectionClazz.getConstructor(String.class);
			return constructor.newInstance(endpointPrefix);
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw new DBException("Reflection Error while Instantiating DB Connection for " + endpointPrefix, e);
		}
	}
}
