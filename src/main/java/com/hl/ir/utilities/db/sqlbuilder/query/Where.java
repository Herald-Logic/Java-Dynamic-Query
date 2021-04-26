package com.hl.ir.utilities.db.sqlbuilder.query;

import com.hl.ir.utilities.db.sqlbuilder.query.builder.Creatable;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.WhereBuilder;

public abstract class Where implements Creatable,Clause{
	
	public static WhereBuilder builder() {
		return new WhereBuilder();
	}
}