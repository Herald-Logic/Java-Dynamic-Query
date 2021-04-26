package com.hl.ir.utilities.db.sqlbuilder.query;

public enum WhereOperator {

	EQUALS("="),
    LESSTHAN("<"),
    GREATERTHAN(">"),
    LESSTHANOREQUALS("<="),
    GREATERTHANOREQUALS(">="),
    NOTEQUALS("<>"),
    IN("IN"),
    NOTIN("NOT IN"),
    BETWEEN("BETWEEN"),
    NOTBETWEEN("NOT BETWEEN"),
	LIKE("LIKE"),
	ILIKE("ILIKE"),
	ISNULL("isnull"),
	ISNOTNULL("is not null");

    public final String label;
    WhereOperator(String value) {
		this.label=value;
	}
}
