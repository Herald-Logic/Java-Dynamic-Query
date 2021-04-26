package com.hl.ir.utilities.db.sqlbuilder.query;

import com.hl.ir.utilities.db.sqlbuilder.query.builder.Creatable;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.DeleteBuilder;

public abstract class Delete implements Query,Creatable {
	
	public static DeleteBuilder builder () {
		return new DeleteBuilder();
	}

}