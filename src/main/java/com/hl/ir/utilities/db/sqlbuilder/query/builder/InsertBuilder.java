package com.hl.ir.utilities.db.sqlbuilder.query.builder;

import java.util.ArrayList;
import java.util.List;

import com.hl.ir.utilities.db.sqlbuilder.query.Insert;
import com.hl.ir.utilities.db.sqlbuilder.query.Select;
import com.hl.ir.utilities.db.sqlbuilder.query.impl.PostgresInsert;
import com.hl.utilities.db.DBType;

public class InsertBuilder implements Builder<Insert> {
	
	private DBType dbType;
	private String table;
	private Select select;
	private List<Object> attributeNames= new ArrayList<Object>();
	private List<Object> attributeValues= new ArrayList<Object>();
	
	public DBType getDBType() {
		return dbType;
	}

	public InsertBuilder dbType(DBType dbType) {
		this.dbType = dbType;
		return this;
	}

	public String getTable() {
		return table;
	}

	public InsertBuilder into(String table) {
		this.table = table;
		return this;
	}

	public List<Object> getAttributeNames() {
		return attributeNames;
	}

	public InsertBuilder attributeNames(List<Object> attributeNames) {
		this.attributeNames = attributeNames;
		return this;
	}

	public List<Object> getAttributeValues() {
		return attributeValues;
	}

	public InsertBuilder attributeValues(List<Object> attributeValues) {
		this.attributeValues = attributeValues;
		return this;
	}
	
	@Override
	public Insert build() {
		switch(this.dbType) {
			case POSTGRES: return new PostgresInsert(this);
			default: return null;
		}
	}

	public Select getSelect() {
		return select;
	}

	public void setSelect(Select select) {
		this.select = select;
	}
}
