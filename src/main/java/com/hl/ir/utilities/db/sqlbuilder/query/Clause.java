package com.hl.ir.utilities.db.sqlbuilder.query;

import com.hl.ir.utilities.db.sqlbuilder.query.builder.Creatable;

public interface Clause extends Creatable{
	/**
	 * This method returns a sql string corresponding to the where clause 
	 * intented by the user.
	 * @return
	 */
	public String fetch();
}
