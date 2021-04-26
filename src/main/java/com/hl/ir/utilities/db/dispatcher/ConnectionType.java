package com.hl.ir.utilities.db.dispatcher;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.hl.ir.utilities.db.builder.DBConnectionDispatcherBuilder;
import com.hl.ir.utilities.db.dispatcher.DBConnectionDispatcher;
import com.hl.ir.utilities.db.exception.DBException;

public enum ConnectionType {
	SIMPLEDBCONNECTION(SimpleDBConnection.class),
	POOLEDDBCONNECTION(PooledDBConnection.class),
	RDSPROXY(RDSProxy.class);
	
	Class<? extends DBConnectionDispatcher> dbConnectionDispatcherClazz;
	
	private ConnectionType(Class<? extends DBConnectionDispatcher> dbConnectionClazz) {
		this.dbConnectionDispatcherClazz = dbConnectionClazz;
	}
	
	public DBConnectionDispatcher getDBConnection(String connectionType, DBConnectionDispatcherBuilder dbConnectionDispatcherBuilder) {
		System.out.println(connectionType);
		try {
			Constructor<? extends DBConnectionDispatcher> constructor = this.dbConnectionDispatcherClazz.getConstructor(DBConnectionDispatcherBuilder.class);
			return constructor.newInstance(dbConnectionDispatcherBuilder);
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			e.printStackTrace();
			throw new DBException("Reflection Error while Instantiating DB Connection for " + connectionType, e);
		}
	}
}
