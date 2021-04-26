package com.hl.ir.utilities.db.dispatcher;

import java.sql.Connection;
import java.sql.SQLException;

public interface DBConnectionDispatcher {
	
	public Connection getConnection() throws SQLException;
}
