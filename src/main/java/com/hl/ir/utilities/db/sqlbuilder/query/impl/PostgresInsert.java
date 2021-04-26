package com.hl.ir.utilities.db.sqlbuilder.query.impl;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import com.hl.ir.utilities.db.builder.DBConnectionDispatcherBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.Insert;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.InsertBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.exception.QueryException;
import com.hl.ir.utilities.db.sqlbuilder.query.response.Response;
import com.hl.ir.utilities.properties.exception.PropertyNotFoundException;
import com.hl.utilities.db.utils.DBUtil;

public class PostgresInsert extends Insert{

	private String table;
	private InsertBuilder builder; 
	private StringBuilder query; 

	public PostgresInsert(InsertBuilder insertBuilder) {
		this.table = insertBuilder.getTable();
		this.builder= insertBuilder;
		query= this.generateInsertQuery(insertBuilder);
	}

	private StringBuilder generateInsertQuery(InsertBuilder insertBuilder) {

		StringBuilder query = new StringBuilder();
		StringBuilder columnNames= new StringBuilder(" ");
		StringBuilder columnValues= new StringBuilder(" ");

		query.append("INSERT INTO ");
		query.append(table);
		insertBuilder.getAttributeNames().stream().forEach((c) -> columnNames.append(c).append(","));	
		query.append("(" +columnNames.substring(0, columnNames.lastIndexOf(","))  +")");
		insertBuilder.getAttributeValues().stream().forEach((c) -> columnValues.append(DBUtil.sqlString(c)).append(","));	
		//insertBuilder.getAttributeValues().stream().forEach((c) -> columnValues.append("?").append(","));	
		query.append(" VALUES (" + columnValues.substring(0, columnValues.lastIndexOf(",")) + ")");	
		System.out.println(query);
		return query;
	}

	@Override
	public Response<?> fire(Connection connection) throws QueryException{
		
		int resultSet = 0;
		boolean releaseConnection = false;
		try {
			if(connection == null) {
				connection = DBConnectionDispatcherBuilder.builder().build().getConnection();
				releaseConnection= true;
			}
			try(PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
				resultSet = preparedStatement.executeUpdate();
			}
			catch (Exception e) {
				e.printStackTrace();
				throw new QueryException(e.getMessage(),e);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage(),e);
		}finally {
			try {
				if(releaseConnection && connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
				throw new QueryException(e.getMessage(),e);
			}
		}
		Integer res= new Integer(resultSet);
		Response<Integer> response= new Response<Integer>();
		response.setResponse(res);
		return response;
	}

	/*@Override
	public Response<?> fire(Connection connection) throws QueryException{

		int resultSet= 0;

		try {
			if(connection == null)
				connection = DBConnectionDispatcherBuilder.builder().build().getConnection();
			try(PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {

				int counter=0;
				for(Object eachColumnValues: builder.getAttributeValues())
				{
					if(eachColumnValues instanceof Integer)
						preparedStatement.setInt(++counter, Integer.parseInt(eachColumnValues.toString()));

					else if(eachColumnValues instanceof Float)
						preparedStatement.setFloat(++counter, Float.parseFloat(eachColumnValues.toString()));					

					else if(eachColumnValues instanceof java.sql.Date)
						preparedStatement.setDate(++counter, new java.sql.Date(((java.sql.Date) eachColumnValues).getTime()));

					else
						preparedStatement.setString(++counter, eachColumnValues.toString());
				}
				resultSet = preparedStatement.executeUpdate();
			} catch (Exception e) {
				e.printStackTrace();
				throw new QueryException(e.getMessage(),e);		    	
			}
		}catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage(),e);
		}finally {
			try {
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
				throw new QueryException(e.getMessage().e);
			}
		}
		Integer res= new Integer(resultSet);
		Response<Integer> response= new Response<Integer>();
		response.setResponse(res);
		return response;
	}*/

	/*@SuppressWarnings("rawtypes")
	private StringBuilder generateInsertQueryJookeNotUsed() throws SQLException, PropertyNotFoundException {

		Connection connection =null;
			if(connection == null)
				connection = DBConnectionDispatcherBuilder.builder().build().getConnection();
		try(PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
			DSLContext create = DSL.using(connection , SQLDialect.POSTGRES);

			Field[] columnNames= new Field[builder.getAttributeNames().size()];
			Field[] columnValues= new Field[builder.getAttributeValues().size()];

			int counter= 0;
			for(Object eachAttributeNames: builder.getAttributeNames())
				columnNames[counter++]= field(field(eachAttributeNames.toString()));

			counter=0;
			for(Object eachAttributeValues: builder.getAttributeValues())
				columnValues[counter++]= field("'"+field(eachAttributeValues.toString())+"'");

			Query jookesQuery= create.insertInto(table(table), 
					columnNames)
					.values(columnValues);

			String sql = jookesQuery.getSQL();
			System.out.println(sql);
			List result =  create.fetchValues(sql);
			System.out.println(result);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}*/
}
