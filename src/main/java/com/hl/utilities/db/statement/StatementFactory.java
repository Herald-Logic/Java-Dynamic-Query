package com.hl.utilities.db.statement;

import com.hl.utilities.db.DBManager;
import com.hl.utilities.db.DBType;
import com.hl.utilities.db.statement.impl.OracleStatement;
import com.hl.utilities.db.statement.impl.PostgresStatement;

@Deprecated
public class StatementFactory {
	
	public Statement createStatement(StatementType statementType) {
		return createStatement(null, statementType, null);
	}
	public Statement createStatement(String endpointName, StatementType statementType) {
		return createStatement(endpointName, statementType, null);
	}
	
	public Statement createStatement(StatementType statementType, String tableName) {
		return createStatement(null, statementType, tableName);
	}
	public Statement createStatement(String endpointName, StatementType statementType, String tableName) {
		Statement statement = null;
//		TEMPORARY CHANGE FOR INCORPORATING MULTITENANCY PHASE 1 CHANGES OF DB UTIL (mandating postgres statement)
//		DBType dbType = DBManager.getInstance().getDbType(endpointName);
//		if(dbType == DBType.POSTGRES)
			statement = new PostgresStatement(statementType, tableName);
//		if(dbType == DBType.ORACLE)
//			statement = new OracleStatement(statementType, tableName);
		return statement;
	}
}
