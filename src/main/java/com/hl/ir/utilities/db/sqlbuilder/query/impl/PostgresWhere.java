package com.hl.ir.utilities.db.sqlbuilder.query.impl;

import com.hl.ir.utilities.db.sqlbuilder.query.Where;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.WhereBuilder;
import static org.jooq.impl.DSL.condition;
import org.jooq.*;

public class PostgresWhere extends Where{

	private String finalQuery;
	private Condition jookesCondition;
	
	public PostgresWhere(WhereBuilder whereBuilder) {
		
		finalQuery=whereBuilder.getCondition().getQuery().toString();
		jookesCondition= condition(finalQuery);
	}

	@Override
	public String fetch() {
		return jookesCondition.toString();
	}
}
