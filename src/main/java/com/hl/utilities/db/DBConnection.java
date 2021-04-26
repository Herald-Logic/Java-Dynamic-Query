package com.hl.utilities.db;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

@Deprecated
public abstract class DBConnection {
	
	public DBType dbType;
	public ConnectionType connectionType;
	
	public DataSource getDataSource() {
			return null;
	}
	
	public ConnectionType getConnectionType() {
		return this.connectionType;
	}

	public DBConnection setConnectionType(ConnectionType connectionType) {
		this.connectionType = connectionType;
		return this;
	}
	
	public DBType getDbType() {
		return this.dbType;
	}

	public DBConnection setDbType(DBType dbType) {
		this.dbType = dbType;
		return this;
	}
	
	abstract public Connection getConnection() throws SQLException;
	abstract public void shutdown();
	
	abstract public Integer maxNoOfConnections();
	abstract public Integer usedNoOfConnections();
}
