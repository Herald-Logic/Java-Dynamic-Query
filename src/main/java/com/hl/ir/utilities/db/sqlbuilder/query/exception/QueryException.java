package com.hl.ir.utilities.db.sqlbuilder.query.exception;

public class QueryException extends Exception{

	private static final long serialVersionUID = 7463313501701049792L;
	
	public QueryException(String message) {
		super(message);
	}
	public QueryException(String message, Throwable throwable) {
		super(message, throwable);
	}
}

