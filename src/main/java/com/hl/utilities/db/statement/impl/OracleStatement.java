package com.hl.utilities.db.statement.impl;

import com.hl.utilities.db.statement.Statement;
import com.hl.utilities.db.statement.StatementType;

@Deprecated
public class OracleStatement extends Statement {
	
	public OracleStatement(StatementType statementType) {
		super(statementType);
	}
	public OracleStatement(StatementType statementType, String tableName) {
		super(statementType, tableName);
	}
}
