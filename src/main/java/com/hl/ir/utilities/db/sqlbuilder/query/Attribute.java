package com.hl.ir.utilities.db.sqlbuilder.query;

public class Attribute implements com.hl.ir.utilities.db.sqlbuilder.structure.Attribute {

	private String column;
	private String alias;
	
	public Attribute(String column, String alias) {
		if(column.contains(".")) {
			String[] columns = column.split("[.]");
			column = columns[0];
			for(int i=1; i<columns.length; i++)
				column += "-> '"+columns[i]+"'";
		}
		this.column = column;
		this.alias = alias;
	}

	public static Attribute attribute(String column, String alias) {
		return new Attribute(column, alias);
	}
	
	public static Attribute attribute(String column) {
		return attribute(column, column);
	}

	public static Attribute jsonb_agg(Attribute attribute) {
		String column = attribute.getColumn();
		if(column.contains("->")) {
			String columns[]= column.split("->");
			column = "jsonb_agg("+columns[0].trim()+" -> cast(ord-1 as int) ";
			for(int i=1; i<columns.length; i++)
				column += " -> "+columns[i].trim();
			column += ")";
		} else
			column = "jsonb_agg("+column+" -> cast(ord-1 as int) )"; 
		attribute.column = column;
		return attribute;
	}
	
	public static Attribute max(Attribute attribute) {
		attribute.column = "max("+attribute.getColumn()+")";
		return attribute;
	}
	
	public String getColumn() {
		return column;
	}

	public String getAlias() {
		return alias;
	}

	public Attribute tableAlias(String tableAlias) {
		this.column = tableAlias+"."+this.column;
		return this;
	}
	
}
