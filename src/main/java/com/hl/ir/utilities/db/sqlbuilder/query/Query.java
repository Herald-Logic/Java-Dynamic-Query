package com.hl.ir.utilities.db.sqlbuilder.query;

import java.sql.Connection;

import com.hl.ir.utilities.db.sqlbuilder.query.exception.QueryException;
import com.hl.ir.utilities.db.sqlbuilder.query.response.Response;

public interface Query {
	Response<?> fire (Connection connection) throws QueryException;
	
	default Response<?> fire() throws QueryException {
		return fire(null);
	}

}
