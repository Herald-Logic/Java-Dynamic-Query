package com.hl.ir.utilities.db.sqlbuilder.query.builder;

import com.hl.ir.utilities.db.sqlbuilder.query.Delete;
import com.hl.ir.utilities.db.sqlbuilder.query.Where;
import com.hl.ir.utilities.db.sqlbuilder.query.impl.PostgresDelete;
import com.hl.utilities.db.DBType;

public class DeleteBuilder implements Builder<Delete> {

	private DBType dbType;
	private String table;
	private String where;

	public DBType getDBType() {
		return dbType;
	}

	public DeleteBuilder dbType(DBType dbType) {
		this.dbType = dbType;
		return this;
	}

	public String getTable() {
		return table;
	}

	public DeleteBuilder from(String table) {
		this.table = table;
		return this;
	}

	public String getWhere() {
		return where;
	}

	public DeleteBuilder where(Where whereObject) {
		where= whereObject.fetch();
		return this;
	}
	
	@Override
	public Delete build() {
		switch(this.dbType) {
			case POSTGRES: return new PostgresDelete(this);
			default: return null;
		}
	}
}
