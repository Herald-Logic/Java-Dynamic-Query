package com.hl.ir.utilities.db.sqlbuilder.query.builder;

import com.hl.ir.utilities.db.sqlbuilder.query.Attribute;
import com.hl.ir.utilities.db.sqlbuilder.query.Select;
import com.hl.ir.utilities.db.sqlbuilder.query.Table;
import com.hl.ir.utilities.db.sqlbuilder.query.Where;
import com.hl.ir.utilities.db.sqlbuilder.query.exception.QueryException;
import com.hl.ir.utilities.db.sqlbuilder.query.impl.PostgresSelect;
import com.hl.utilities.db.DBType;

public class SelectBuilder implements Builder<Select> {
	
	private DBType dbType;
	private Table[] table;
	private String where;
	private Attribute[] attributes;
	
	@Override
	public Select build() throws QueryException {
		switch(this.dbType) {
		case POSTGRES: return new PostgresSelect(this);			
		default: return null;
		}
	}
	
	public SelectBuilder dbType(DBType type) {
		this.dbType = type;
		return this;
	}
	
	public SelectBuilder from(Table ... table) {
		this.table = table;
		return this;
	}
	
	public SelectBuilder where(Where where) {
		this.where = where.fetch();
		return this;
	}
	
	public SelectBuilder attributes(Attribute ... attributes) {
		this.attributes = attributes;
		return this;
	}

	public Table[] getTable() {
		return table;
	}

	public String getWhere() {
		return where;
	}

	public Attribute[] getAttributes() {
		return attributes;
	}
	
}
