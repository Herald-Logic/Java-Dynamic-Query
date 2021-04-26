package com.hl.ir.utilities.db.sqlbuilder.query.builder;

import com.hl.ir.utilities.db.sqlbuilder.query.exception.QueryException;

public interface Builder <T extends Creatable> {
	T build () throws QueryException;
}
