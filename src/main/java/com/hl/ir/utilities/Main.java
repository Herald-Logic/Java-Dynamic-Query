package com.hl.ir.utilities;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.hl.ir.utilities.db.builder.DBConnectionDispatcherBuilder;
import com.hl.ir.utilities.db.dispatcher.DBConnectionDispatcher;
import com.hl.ir.utilities.db.sqlbuilder.query.Condition;
import com.hl.ir.utilities.db.sqlbuilder.query.Delete;
import com.hl.ir.utilities.db.sqlbuilder.query.Insert;
import com.hl.ir.utilities.db.sqlbuilder.query.Where;
import com.hl.ir.utilities.db.sqlbuilder.query.WhereOperator;
import com.hl.ir.utilities.db.sqlbuilder.query.builder.WhereBuilder;
import com.hl.ir.utilities.db.sqlbuilder.query.response.Response;
import com.hl.ir.utilities.properties.Client;
import com.hl.ir.utilities.properties.exception.PropertyNotFoundException;
import com.hl.utilities.db.DBType;

import static com.hl.ir.utilities.db.sqlbuilder.query.Condition.*;

public class Main {

	public static void main(String[] args) throws Exception {
		
		System.out.println("IN MAIN CLASS");
		
		dbConnection();
		//whereClause();
		//insertQuery();
		//deleteQuery();
	}	

	private static void whereClause() throws Exception {

		Where where= Where.builder()
					.dbType(DBType.POSTGRES)
					.set(condition(""))
					.build();
		
		System.out.println(where.fetch());
	}

	private static void complexWhereClause() throws Exception
	{
		WhereBuilder builder=  Where.builder()
				.dbType(DBType.POSTGRES)
				.set(
						condition("=","book_id","book_name").and(
							condition("<", "col1", "col2").or(
									Condition.condition("=","col3","col4")
									.and(condition("=","3","3")))
							
						).or(condition("=","book_id","book_name")));
						System.out.println(builder.build().fetch());
	}
	
	private static void dbConnection() throws Exception
	{
		Thread t1= new Thread(() ->{
			try {
				Client.builder().domain("wlh").client("demo").build();
			} catch (PropertyNotFoundException e1) {
				e1.printStackTrace();
			}

			try{
				DBConnectionDispatcher  builder1 = DBConnectionDispatcherBuilder.builder().build();
				System.out.println("builder1:" + builder1.hashCode());
				
					Connection connection1 = builder1.getConnection();					
			}
			catch (Exception e) {
				e.printStackTrace();				
			}
		});
		
		t1.start();

	}
	private static void insertQuery() throws Exception {
		
		System.out.println("in insert");
		String jsonStringObject= "{\"key1\":{\"key3\":10,\"key4\":{\"key5\":\"11\",\"key6\":150}},\"key2\":{\"key7\":\"1000\",\"key8\":\"222\"},\"key9\":1222,\"unique_key\":12}";
		String jsonStringArray= "[{\"key1\": {\"key3\": 500, \"key4\": {\"key6\": 150}}, \"key2\": {\"key7\": \"1000\", \"key8\": \"222\"}, \"key9\": 1222, \"unique_key\": 12}, {\"key1\": {\"key3\": 15, \"key4\": {\"key5\": 2265, \"key6\": 15}}, \"key2\": {\"key7\": \"15\"}, \"unique_key\": 13}, {\"key1\": {\"key3\": 20, \"key4\": {\"key5\": 20}}, \"key2\": {\"key7\": \"10\"}, \"unique_key\": 14}, {\"key1\": {\"key3\": 15, \"key4\": {\"key5\": 25, \"key6\": 15}}, \"key2\": {\"key7\": \"15\"}, \"unique_key\": 15}, {\"key1\": {\"key3\": 200, \"key4\": {\"key5\": 520}}, \"key2\": {\"key7\": \"10\"}, \"unique_key\": 16}, {\"key1\": {\"key3\": 15, \"key4\": {\"key5\": 256, \"key6\": 15}}, \"key2\": {\"key7\": \"15\"}, \"unique_key\": 17}]";
	
		JsonObject jsonObject= new Gson().fromJson(jsonStringObject, JsonObject.class);
		JsonArray jsonArray= new Gson().fromJson(jsonStringArray, com.google.gson.JsonArray.class);

		List<Object> attributeNames= new ArrayList<Object>();
		List<Object> attributeValues= new ArrayList<Object>();
		attributeNames.add("book_id");attributeNames.add("author_id");attributeNames.add("published_in");
		attributeNames.add("title");attributeNames.add("jsonb_column");
		attributeNames.add("stringarray_column");
		
		attributeValues.add(78000);attributeValues.add(97);attributeValues.add(2021);
		attributeValues.add("zzz");attributeValues.add(jsonObject);
		attributeValues.add(jsonArray);
			
		Insert insert= Insert
					.builder()
					.dbType(DBType.POSTGRES)
					.into("book")
					.attributeNames(attributeNames)
					.attributeValues(attributeValues)
					.build();
		Response<Integer> res= (Response<Integer>) insert.fire();
		System.out.println(res.getResponse());
	}
	
	private static void deleteQuery() throws Exception
	{

		Where builder=  Where.builder()
				.dbType(DBType.POSTGRES)
				.set(
						condition(WhereOperator.IN,"book_id", "1000,2000",true)
					)
				.build();
				
		Delete delete= Delete
						.builder()
						.dbType(DBType.POSTGRES)
						.from("book")
						.where(builder)
						.build();
		Response<List> res=(Response<List>) delete.fire();
		System.out.println(res.getResponse());
	}
}
