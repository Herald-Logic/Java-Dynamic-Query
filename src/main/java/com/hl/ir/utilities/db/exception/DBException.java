package com.hl.ir.utilities.db.exception;

public class DBException extends RuntimeException {

	private static final long serialVersionUID = 7463313501701049792L;
	
	public DBException(String message) {
		super(message);
	}
	public DBException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
