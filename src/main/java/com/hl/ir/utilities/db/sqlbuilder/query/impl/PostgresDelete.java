package com.hl.ir.utilities.db.sqlbuilder.query.impl;

import static org.jooq.impl.DSL.table;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.DeleteUsingStep;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import com.hl.ir.utilities.db.builder.DBConnectionDispatcherBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.Delete;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.DeleteBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.exception.QueryException;
import com.hl.ir.utilities.db.sqlbuilder.query.response.Response;

public class PostgresDelete extends Delete{

	private String tableName;
	private DeleteBuilder builder; 

	public PostgresDelete(DeleteBuilder deleteBuilder) {
		tableName= deleteBuilder.getTable();
		this.builder= deleteBuilder;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Response<?> fire(Connection connection) throws QueryException {
		boolean releaseConnection = false;

		List result= null;
		try {
			if(connection == null) {
				connection = DBConnectionDispatcherBuilder.builder().build().getConnection();
				releaseConnection= true;
			}
			DSLContext create = DSL.using(connection, SQLDialect.POSTGRES);

			String whereClause= builder.getWhere();
			DeleteUsingStep<Record> deleteRecords= create.deleteFrom(table(tableName));
			if(whereClause != null)
				deleteRecords.where(whereClause);
			Query query= deleteRecords;
			String sql = query.getSQL();
			System.out.println(sql);
			result=  create.fetchValues(sql);
		} catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage());
		} finally {
			try {
				if(releaseConnection && connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
				throw new QueryException(e.getMessage(),e);
			}
		}
		Response<List> response= new Response<List>();
		response.setResponse(result);
		return response;
	}
}
