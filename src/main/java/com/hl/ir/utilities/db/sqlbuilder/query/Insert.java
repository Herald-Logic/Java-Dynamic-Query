package com.hl.ir.utilities.db.sqlbuilder.query;

import com.hl.ir.utilities.db.sqlbuilder.query.builder.Creatable;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.InsertBuilder;

public abstract class Insert implements Query,Creatable {

	public static InsertBuilder builder () {
		return new InsertBuilder();
	}
}
