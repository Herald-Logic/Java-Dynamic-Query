package com.hl.ir.utilities.db.sqlbuilder.query;

public class Table implements com.hl.ir.utilities.db.sqlbuilder.structure.Table {

	private String tableName;
	private String alias;
	private Table join;
	private String on;
	private Select nestedJoin;
	
	public Table(String tableName, String alias) {
		this.tableName = tableName;
		this.alias = alias;
	}
	
	public Table(Table table) {
		this.tableName = table.tableName;
		this.alias = table.alias;
		this.join = table.join;
		this.on = table.on;
		this.nestedJoin = table.nestedJoin;
	}

	public static Table table(String tableName, String alias) {
		return new Table(tableName, alias);
	}
	
	public static Table table(String tableName) {
		return table(tableName, tableName.substring(0,1));
	}
	
	public Table innerJoin(Table table) {
		this.join = table;
		return this;
	}
	
	public Table on(String condition) {
		this.on = condition;
		return this;
	}
	
	public String getTableName() {
		return tableName;
	}

	public String getAlias() {
		return alias;
	}

	public Table getJoin() {
		return join;
	}

	public String getOn() {
		return on;
	}

	public Table innerJoin(Select inner) {
		this.nestedJoin = inner;
		return this;
	}

	public Select getNestedJoin() {
		return nestedJoin;
	}

	public static Table jsonb_array_elements(String column) {
		return table("jsonb_array_elements("+column+") with ordinality as arr(opt, ord)", null);
	}

}
