package com.hl.ir.utilities.db.sqlbuilder.query.builder;

import com.hl.ir.utilities.db.sqlbuilder.query.Clause;
import com.hl.ir.utilities.db.sqlbuilder.query.Where;
import com.hl.ir.utilities.db.sqlbuilder.query.impl.PostgresWhere;
import com.hl.utilities.db.DBType;
import com.hl.ir.utilities.db.sqlbuilder.query.Condition;

public class WhereBuilder implements Builder<Clause>{

	private DBType dbType;
	private Condition condition;

	public DBType getDBType() {
		return dbType;
	}

	public WhereBuilder dbType(DBType dbType) {
		this.dbType = dbType;
		return this;
	}
		
	@Override
	public Where build() {
		switch(this.dbType) {
			case POSTGRES: return new PostgresWhere(this);
			default: return null;
		}
	}
		
	public WhereBuilder set(Condition condition)
	{
		this.setCondition(condition);
		return this;
	}

	public Condition getCondition() {
		return condition;
	}

	public void setCondition(Condition condition) {
		this.condition = condition;
	}
}