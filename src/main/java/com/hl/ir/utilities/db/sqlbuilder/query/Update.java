package com.hl.ir.utilities.db.sqlbuilder.query;

import com.hl.ir.utilities.db.sqlbuilder.query.builder.Creatable;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.UpdateBuilder;

public abstract class Update implements Query,Creatable {

	public static UpdateBuilder builder () {
		return new UpdateBuilder();
	}
}
