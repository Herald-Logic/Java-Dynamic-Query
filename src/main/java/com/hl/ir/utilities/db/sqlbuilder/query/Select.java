package com.hl.ir.utilities.db.sqlbuilder.query;

import com.hl.ir.utilities.db.sqlbuilder.query.builder.Creatable;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.SelectBuilder;

public abstract class Select implements Query,Creatable {

	public static SelectBuilder builder () {
		return new SelectBuilder();
	}
}
