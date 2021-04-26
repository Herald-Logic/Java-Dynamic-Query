package com.hl.ir.utilities.db.sqlbuilder.query.response;

public class Response<T> {
	
	private T response;
	
	public T response () {
		return response;
	}

	public T getResponse() {
		return response;
	}

	public void setResponse(T response) {
		this.response = response;
	}
	
}
