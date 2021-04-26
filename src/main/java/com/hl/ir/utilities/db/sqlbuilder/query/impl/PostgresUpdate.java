package com.hl.ir.utilities.db.sqlbuilder.query.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.hl.ir.utilities.db.builder.DBConnectionDispatcherBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.Update;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.UpdateBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.exception.QueryException;
import com.hl.ir.utilities.db.sqlbuilder.query.response.Response;

public class PostgresUpdate extends Update {

	private Map<String, String> concat;
	private Map<String, List<String>> pipe;
	private StringBuilder query;
	private String table;
	private String where;
	private static String ORDINALITY_QUERY = "(select case ordi.pos when ordi.pos then cast(ordi.pos as int4) else 9999 end from ( select ( select cast(ord-1 as int) as pos from <table_name> b , jsonb_array_elements(b.<column_name>) with ordinality as arr(opt, ord) <where>)) as ordi)";

	public PostgresUpdate(UpdateBuilder updateBuilder) throws QueryException {
		try {
			where = updateBuilder.getWhere();
			this.table = updateBuilder.getTable();
			query = generateUpdateQuery(updateBuilder.getUpdateMap());
			System.out.println("Update Query: " + query);
		} catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage());
		}
	}

	@Override
	public Response<?> fire(Connection connection) throws QueryException {
		boolean releaseConnection = false;		
		try {
			if(connection == null) {
				connection = DBConnectionDispatcherBuilder.builder().build().getConnection();
				releaseConnection= true;
			}
			try (PreparedStatement pst= connection.prepareStatement(query.toString())) {
				Response<Integer> i = new Response<Integer>();
				i.setResponse(pst.executeUpdate());
				return i;
			} catch (Exception e) {
				e.printStackTrace();
				throw new QueryException(e.getMessage());
			}
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

	private StringBuilder generateUpdateQuery(Map<String, Object> map) throws QueryException {
		try {
			StringBuilder query = new StringBuilder();
			query.append("update "+this.table+" set ");
			for(String columnName : map.keySet()) {
				if(map.get(columnName) instanceof JsonObject) {
					if(query.toString().substring(query.toString().indexOf("set ")+4).length() > 0)
						query.append(", ");
					query = query.append(" "+columnName+" =	coalesce( "+columnName+" ");
					generateJsonObjectQuery(query, columnName, map.get(columnName));
					query = query.append(")");
				}
				else if(map.get(columnName) instanceof JsonArray) {
					if(query.toString().substring(query.toString().indexOf("set ")+4).length() > 0)
						query.append(", ");
					query = query.append(" "+columnName+" = ");
					generateJsonArrayQuery(query, columnName, map.get(columnName));
					if(StringUtils.countMatches(query, "(") - StringUtils.countMatches(query, ")") == 1)
						query = query.append(")");
				}
				else {
					if(query.toString().substring(query.toString().indexOf("set ")+4).length() > 0)
						query.append(", ");
					query.append(" "+columnName+" = "+"'"+map.get(columnName)+"'");
				}
			}
			query.append(this.where);
			//		System.out.println("--------------QUERY: "+query);
			return query;
		} catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage());
		}
	}


	private void generateJsonArrayQuery(StringBuilder query, String columnName, Object object) throws QueryException {
		try {
			StringBuilder previousjsonbset = null;

			String action = "action:upsert";
			int deleteCount = 0;
			for(JsonElement jsonElement: (JsonArray)object) {
				if(jsonElement instanceof JsonObject) {
					JsonObject jsonObject = jsonElement.getAsJsonObject();
					List<String> uniqueIds = new Gson().fromJson(jsonObject.get("unique_ids").getAsJsonArray(), List.class);
					String ordinalityQuery = new String(ORDINALITY_QUERY);
					ordinalityQuery = ordinalityQuery.replaceAll("<table_name>", this.table);
					ordinalityQuery = ordinalityQuery.replaceAll("<column_name>", columnName);
					String where = new String(this.where);
					for(String uniqueId: uniqueIds) {
						where = where + " and opt ->> '"+uniqueId+ "' = '"+jsonObject.get(uniqueId).getAsNumber()+"'";
					}
					ordinalityQuery = ordinalityQuery.replaceAll("<where>", where);

					switch(action){
					case "action:upsert": {
						StringBuilder jsonbset = new StringBuilder();
						if(previousjsonbset == null) {
							jsonbset.append("jsonb_set( "+columnName+", cast('{' || "+ordinalityQuery+" || '}' as text[]), coalesce( coalesce( "+columnName+" -> "+ordinalityQuery+", '{}'::jsonb) ");
							generateJsonObjectQuery(jsonbset, columnName+" -> "+ordinalityQuery, jsonObject);
							jsonbset.append(" ) ");
							previousjsonbset = new StringBuilder(jsonbset);
						}
						else {
							StringBuilder temp = new StringBuilder();
							jsonbset.append("jsonb_set( "+previousjsonbset + "), cast('{' || "+ordinalityQuery+" || '}' as text[]), coalesce( coalesce( "+columnName+" -> "+ordinalityQuery+", '{}'::jsonb) ");
							generateJsonObjectQuery(temp, columnName+" -> "+ordinalityQuery, jsonObject);
							jsonbset.append(temp);
							jsonbset.append(" )");
							previousjsonbset = new StringBuilder(jsonbset);
						}
						break;
					}
					case "action:delete": {
						if(previousjsonbset == null) {
							previousjsonbset = new StringBuilder();
							previousjsonbset.append("( "+columnName+" ");
							previousjsonbset.append(" - "+ordinalityQuery);
						}
						else {
							previousjsonbset.append(" - "+ordinalityQuery);
						}
						break;
					}
					}
				} else if (jsonElement instanceof JsonPrimitive) {
					String string = jsonElement.getAsString();
					if(string.contains("action:")) {
						action = string;
					} else {
						String ordinalityQuery = new String(ORDINALITY_QUERY);
						ordinalityQuery = ordinalityQuery.replaceAll("<table_name>", this.table);
						ordinalityQuery = ordinalityQuery.replaceAll("<column_name>", columnName);
						String where = new String(this.where);
						where = where + " and opt = '\""+string+"\"'";
						ordinalityQuery = ordinalityQuery.replaceAll("<where>", where);

						switch(action) {
						case "action:upsert": {
							StringBuilder jsonbset = new StringBuilder();
							if(previousjsonbset == null) {
								jsonbset.append("jsonb_set( "+columnName+", cast('{' || "+ordinalityQuery+" || '}' as text[]), coalesce( "+columnName+" -> "+ordinalityQuery+" ");
								jsonbset.append(", '\""+string+"\"') )");
								previousjsonbset = new StringBuilder(jsonbset);
							}
							else {
								jsonbset.append("jsonb_set( "+previousjsonbset+", cast('{' || "+ordinalityQuery+" || '}' as text[]), coalesce( "+columnName+" -> "+ordinalityQuery+" ");
								jsonbset.append(", '\""+string+"\"') ");
								previousjsonbset = new StringBuilder(jsonbset);
							}
							break;
						}
						case "action:delete": {
							if(previousjsonbset == null) {
								previousjsonbset = new StringBuilder();
								previousjsonbset.append("( "+columnName+" ");
								previousjsonbset.append(" - '"+string+"' ");
							}
							else {
								previousjsonbset.append(" - '"+string+"' ");
							}
							deleteCount++;
							break;
						}
						}
					}
				}
			}
			query.append(previousjsonbset);
		} catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage());
		}
	}

	private void generateJsonObjectQuery(StringBuilder query, String columnName, Object columnInputValue) throws QueryException {
		try {
			concat = new HashMap<String, String>();
			pipe = new HashMap<String, List<String>>();
			recursive(columnName, (JsonObject) columnInputValue);
			//			System.out.println("CONCAT: _------------------------------");

			List<String> tempList = new ArrayList<>();
			for(String key: concat.keySet()) {
				tempList.add(key);
			}
			Collections.sort(tempList);
			Map<String, Boolean> tempMap = new HashMap<>();
			for(String key: tempList) {
				tempMap.put(key, false);
			}

			if(pipe.containsKey(columnName)) {
				List<String> pipelist = pipe.get(columnName);
				for(String pipestring : pipelist)
					query = query.append(pipestring);
			}
			int concatTraversed = StringUtils.countMatches(query, "concat");
			for(String key: tempList) {
				formQuery(tempMap, key, query);
				for(String tempKey : tempList) {
					if(tempKey.contains(key)) {
						formQuery(tempMap, tempKey, query);
						tempMap.put(key, true);
					}
				}
				int concat = StringUtils.countMatches(query, "concat");
				for(int i=0; i<concat - concatTraversed; i++)
					query = query.append("	)  #>> '{}' , '}') ::jsonb ");
				concatTraversed = concat;
			}

			//			System.out.println("PIPE: _------------------------------");
			//			pipe.forEach((key, value)->{
			//				System.out.println(key+"                                                ");
			//				value.forEach((val)->{
			//					System.out.println(val);
			//				});
			//
			//			});
		} catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage());
		}

	}

	private void formQuery(Map<String, Boolean> tempMap, String key, StringBuilder query) throws QueryException {
		try {
			if(tempMap.get(key) == false) {

				tempMap.put(key, true);
				//			System.out.println(key+"                                                "+concat.get(key));
				query = query.append(" || " + concat.get(key));
				List<String> s = pipe.get(key);
				if(s!=null) {
					for(String pipestring: s) {
						query = query.append(pipestring);
					}
				}
			} 
		} catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage());
		}
	}

	private void recursive(String path, JsonObject obj) throws QueryException {
		try {
			for(String key: obj.keySet()) {
				if(!key.equals("unique_ids")) {
					if(obj.get(key).isJsonObject()) {
						concat.put(path+" -> '"+key+"'", "concat ( '{\""+key+"\" : ', coalesce (coalesce( "+path+" -> '"+key+"', '{}'::jsonb)");
						String recpath = path + " -> " + "'"+key+"'";
						recursive(recpath, obj.get(key).getAsJsonObject());
					}
					else {
						if(pipe.get(path) != null) {
							List<String> s = pipe.get(path);
							s.add("|| '{\""+key+"\":"+obj.get(key)+"}'");
							pipe.put(path, s);
						}
						else {
							List<String> s = new ArrayList<String>();
							s.add("|| '{\""+key+"\":"+obj.get(key)+"}'");
							pipe.put(path, s);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new QueryException(e.getMessage());
		}
	}
	
	@Override
	public String toString() {
		return query.toString();
	}
}
