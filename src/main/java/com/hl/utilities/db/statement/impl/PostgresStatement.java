package com.hl.utilities.db.statement.impl;

import com.hl.utilities.db.statement.Statement;
import com.hl.utilities.db.statement.StatementType;

@Deprecated
public class PostgresStatement extends Statement {
	
	public PostgresStatement(StatementType statementType) {
		super(statementType);
	}
	public PostgresStatement(StatementType statementType, String tableName) {
		super(statementType, tableName);
	}
}
