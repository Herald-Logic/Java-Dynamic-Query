package com.hl.utilities.db.statement.exception;

@Deprecated
public class InvalidStatementException extends RuntimeException {

	private static final long serialVersionUID = 5206892495572144294L;
	
	public InvalidStatementException(String message) {
		super(message);
	}
	public InvalidStatementException(String message, Throwable e) {
		super(message,e);
	}
}
