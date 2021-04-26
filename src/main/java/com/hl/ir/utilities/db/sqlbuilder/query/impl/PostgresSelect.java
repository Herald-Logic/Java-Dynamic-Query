package com.hl.ir.utilities.db.sqlbuilder.query.impl;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectJoinStep;
import org.jooq.SelectSelectStep;
import org.jooq.impl.DSL;
import org.postgresql.util.PGobject;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.hl.ir.utilities.db.builder.DBConnectionDispatcherBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.Attribute;
import com.hl.ir.utilities.db.sqlbuilder.query.Select;
import com.hl.ir.utilities.db.sqlbuilder.query.Table;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.SelectBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.exception.QueryException;
import com.hl.ir.utilities.db.sqlbuilder.query.response.Response;
import com.hl.ir.utilities.properties.exception.PropertyNotFoundException;

public class PostgresSelect extends Select {

	List<Field> fields= new ArrayList<Field>();
	SelectJoinStep<Record> finalQuery;
	Table[] table;
	Attribute[] attributes;
	String where;

	public PostgresSelect(SelectBuilder selectBuilder) throws QueryException {
		try{
			this.table = selectBuilder.getTable();
			this.attributes = selectBuilder.getAttributes();
			this.where = selectBuilder.getWhere();

			DSLContext context = DSL.using(SQLDialect.POSTGRES);

			if(this.attributes != null) {
				for(Attribute attribute: this.attributes) {
					Field<Object> field = field(attribute.getColumn());
					field = field.as(name(attribute.getAlias()));
					fields.add(field);
				}
			}
			org.jooq.Table<Record> jooqTable = null;

			SelectSelectStep<Record> record = context.select(this.fields); //.innerJoin(context.select()).on(""));

			for(Table table: table) {
				if(jooqTable == null) {
					jooqTable = table(table.getTableName());
					if(table.getAlias() != null)
						jooqTable = jooqTable.as(name(table.getAlias()));
				}
				//--------------------
				Table tempTable = new Table(table);
				if(table.getJoin() != null) {
					while(tempTable.getJoin() != null) {
						org.jooq.Table<Record> jooqJoinTable = table(tempTable.getJoin().getTableName());
						jooqJoinTable = jooqJoinTable.as(name(tempTable.getJoin().getAlias()));
						jooqTable = jooqTable.innerJoin(jooqJoinTable).on(tempTable.getOn());
						tempTable = new Table(tempTable.getJoin());
					}
					finalQuery = record.from(jooqTable);
				} else {
					jooqTable = table(table.getTableName());
					if(table.getAlias() != null)
						jooqTable = jooqTable.as(name(table.getAlias()));
					finalQuery = record.from(jooqTable);
				}
				//--------------------
				//			if(table.getJoin() != null) {
				//				org.jooq.Table<Record> selectRecord = null;
				//				org.jooq.Table<Record> jooqJoinTable = table(table.getJoin().getTableName());
				//				jooqJoinTable = jooqJoinTable.as(name(table.getJoin().getAlias()));
				//				if(table.getJoin().getJoin() != null) {
				//					String[] on = table.getOn().split("[.]");
				//					String alias;
				//					if(!on[0].equals(table.getAlias()))
				//						alias = on[0];
				//					else
				//						alias = on[1].split(" ")[2];
				//					selectRecord = createNestedJoin(context, record, table.getJoin(), alias);
				//					finalQuery = record.from(jooqTable.innerJoin(selectRecord).on(table.getOn()));
				//				} else {
				//					jooqTable = jooqTable.innerJoin(jooqJoinTable).on(table.getOn());
				//					finalQuery = record.from(jooqTable);
				//				}
				//			}
				//			else {
				//				jooqTable = table(table.getTableName());
				//				if(table.getAlias() != null)
				//					jooqTable = jooqTable.as(name(table.getAlias()));
				//				finalQuery = record.from(jooqTable);
				//			}
			}
			if(where != null)
				finalQuery.where(where);
			System.out.println(finalQuery);
		} catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage());
		}
	}

	@Override
	public String toString() {
		return finalQuery.toString();
	}

	@Override
	public Response<List<Map<String, Object>>> fire(Connection connection) throws QueryException {
		boolean releaseConnection = false;
		try {
			if(connection == null) {
				connection = DBConnectionDispatcherBuilder.builder().build().getConnection();
				releaseConnection= true;
			}
			DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);

			Result<Record> g = context.fetch(finalQuery);
			List<Map<String, Object>> response= new ArrayList<Map<String,Object>>();
			Map<String, Object> map;
			for(Record r: g) {
				map = new HashMap<>();

				Map<String, Object> intoMap = r.intoMap();
				for(String k: intoMap.keySet()) {
					Object v = intoMap.get(k);
					Object value = v;
					if(v instanceof PGobject) {
//						Handle record type here
//						if(((PGobject) v).getValue().startsWith("("))
//							value = new Gson().fromJson(((PGobject) v).getValue(), Result.class);else 
						if(((PGobject) v).getValue().startsWith("{"))
							value = new Gson().fromJson(((PGobject) v).getValue(), JsonObject.class);
						else if(((PGobject) v).getValue().startsWith("["))
							value = new Gson().fromJson(((PGobject) v).getValue(), JsonArray.class);
					}
					map.put(k, value);
				}
				response.add(map);
			}
			//			List<Map<String, Object>> i = finalQuery.fetchMaps();
			response.forEach((mapp)->{
				System.out.println("-------------------------------------------------------------------------------------------------------------");
				mapp.forEach((k,v)->{
					System.out.println(k+" : "+v);
				});
			});
			//			JsonObject result = new JsonObject();
			//			for(Record r:i) {
			//				Row a= r.fieldsRow();
			//				for(Field<?> b: a.fields()) {
			//					if(b.toString().contains(".")) {
			//						String key[] = b.toString().split("[.]");
			//						if(b.getValue(r) != null) {
			//							if(result.has(key[0].replaceAll("\"", ""))) {
			//								JsonObject js = result.get(key[0].replaceAll("\"", "")).getAsJsonObject();
			//								js.addProperty(key[1].replaceAll("\"", ""), b.getValue(r).toString());
			//								result.add(key[0].replaceAll("\"", ""), js);
			//							} else {
			//								JsonObject js = new JsonObject();
			//								js.addProperty(key[1].replaceAll("\"", ""), b.getValue(r).toString());
			//								result.add(key[0].replaceAll("\"", ""), js);
			//							}
			//						}
			//					}
			//				}
			//			}
			//			System.out.println(result);
			Response<List<Map<String, Object>>> result = new Response<List<Map<String,Object>>>();
			result.setResponse(response);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage());
		} finally {
			try {
				if(releaseConnection && connection != null)
					connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new QueryException(e.getMessage());
			}
		}
	}

	//	----------------------------------------------------------------------------------
	//	select root -> inner join ( select nonroot1 inner join nonroot2 on cond ) on cond 
	//	global table = root, root.getJoin() = nonroot1.getJoin() = nonroot2
	//	----------------------------------------------------------------------------------

	//	private org.jooq.Table<Record> createNestedJoin(DSLContext context, SelectSelectStep<Record> record, Table table, String alias) {
	//		//		Table t = ((PostgresSelect)nestedJoin).getTable();
	//		org.jooq.Table<Record> jooqTempTable;
	//		jooqTempTable = table(table.getTableName());
	//		jooqTempTable = jooqTempTable.as(name(table.getAlias()));
	//
	//		org.jooq.Table<Record> jooqJoinTable = table(table.getJoin().getTableName());
	//		jooqJoinTable = jooqJoinTable.as(name(table.getJoin().getAlias()));
	//
	//		String[] on = table.getOn().split("[.]");
	//		String alias1;
	//		if(!on[0].equals(table.getAlias()))
	//			alias1 = on[0];
	//		else
	//			alias1 = on[1].split(" ")[2];
	//
	//		org.jooq.Table<Record> selectjoinstep;
	//		if(table.getJoin().getJoin() != null) {
	//			jooqTempTable = jooqTempTable.innerJoin(createNestedJoin(context, record, table.getJoin(), alias1)).on(table.getOn());
	//			selectjoinstep = context.select().from(jooqTempTable).asTable(alias);
	//		}
	//		else {
	//			jooqTempTable = jooqTempTable.innerJoin(jooqJoinTable).on(table.getOn());
	//			selectjoinstep = context.select().from(jooqTempTable).asTable(alias);
	//		}
	//		return selectjoinstep;
	//	}
}
