package com.hl.ir.utilities.db.sqlbuilder.query;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
public class Condition {

	private StringBuilder query;
	
	public Condition() throws Exception {
		query= new StringBuilder();
	}
	public Condition(WhereOperator operator,String columnName,Object columnValue, boolean isAttribute, boolean isJson) throws Exception {
		query= new StringBuilder();
		set(operator, columnName, columnValue,isAttribute,isJson);
	}
	
	public static Condition condition(String query) throws Exception {
		
		Condition plainCondition= new Condition();
		plainCondition.query=new StringBuilder(query.toString());
		return plainCondition;
	}
	
	public static Condition condition(String operatorString, String columnName, Object columnValue) throws Exception {
		
		WhereOperator whereOperator= validateOperator(operatorString);
		if(whereOperator == null) throw new Exception("in-valid opertor");
		return condition(whereOperator, columnName, columnValue);
	}
	
	public static Condition condition(String operatorString, String columnName, Object columnValue, boolean isAttribute, boolean isJsonb) throws Exception {
		
		WhereOperator whereOperator= validateOperator(operatorString);
		if(whereOperator == null) throw new Exception("in-valid opertor");
		return condition(whereOperator, columnName, columnValue, isAttribute, isJsonb);
	}
	
	public static Condition condition(String columnName, Object columnValue,boolean isAttribute) throws Exception
	{
		return condition(columnName,columnValue,isAttribute,false);
	}
	
	public static Condition condition(String columnName, Object columnValue,boolean isAttribute, boolean isJson) throws Exception
	{
		return condition(WhereOperator.EQUALS,columnName,columnValue,isAttribute,isJson);
	}
	
	public static Condition condition(String columnName,Object columnValue) throws Exception
	{
		return condition(WhereOperator.EQUALS, columnName,columnValue);
	}
	
	public static Condition condition(WhereOperator operator,String columnName,Object columnValue) throws Exception
	{
		return condition(operator, columnName,columnValue,false);
	}
	
	public static Condition condition(WhereOperator operator,String columnName, Object columnValue,boolean isAttribute) throws Exception
	{
		return condition(operator, columnName,columnValue,isAttribute,false);
	}

	public static Condition condition(WhereOperator operator,String columnName,Object columnValue,boolean isAttribute, boolean isJson) throws Exception
	{
		return new Condition(operator, columnName,columnValue,isAttribute,isJson);
	}
		
	public Condition and(Condition and) {
		
		query.append(" AND (").append(and.query).append(" )");
		return this;
	}
	
	public Condition or(Condition or) {
		query.append(" OR (").append(or.query).append(" )");
		return this;
	}
	
	private Condition set(WhereOperator operator, String columnName, Object columnValue,boolean isAttribute,boolean isJson) throws Exception {
		
		String whereOperator= operator.label.toString();
		boolean inOperators= false;
		if(whereOperator.equalsIgnoreCase(WhereOperator.IN.toString())
				|| whereOperator.equalsIgnoreCase(WhereOperator.NOTIN.toString()))
			inOperators=true;
		
		if(isJson)
		{
			boolean instanceOfJson= false;
			if(columnValue instanceof String)
				instanceOfJson= testStringIFInstanceOfJson(columnValue.toString());
			columnName=manipuateJSONKeys(columnName,instanceOfJson);	
			
			if(isAttribute)
			{
				long count = columnValue.toString().chars().filter(ch -> ch == '.').count();
				if(count>0)columnValue=manipuateJSONKeys(columnValue,false);	
			}
		}
		
		query.append(columnName)
		.append(" ")
		.append(whereOperator)
		.append(" ");
		
		query= inOperators ? query.append("("):query;
		query= isAttribute ? query:query.append("'");
		query.append(columnValue.toString());
		query= isAttribute ? query:query.append("'");		
		query= inOperators ? query.append(")"):query;
		return this;
	}
	
	private String manipuateJSONKeys(Object jsonKeys, boolean instanceOfJson) throws Exception {

		if(!jsonKeys.toString().contains("."))
			throw new Exception("json keys should be separated with \".\" eg:- table_name.column_name.key1..."); 
		
		String arrColKey[]= jsonKeys.toString().split("\\.");
		String manipulatedKey= new String();
		
		if(arrColKey.length == 2)
			return jsonKeys.toString();
		else 
			manipulatedKey= concatJSONBKey(arrColKey,instanceOfJson);
		return manipulatedKey;
	}
	
	private String concatJSONBKey(String[] arrColKey, boolean instanceOfJson)
	{
		String concatColKey= arrColKey[0].concat(".").concat(arrColKey[1]);
		
		int counter=0;
		boolean numeric=false;
		String lastKey="";
		for(String eachKey : arrColKey) {
			counter++;
			numeric= eachKey.matches("-?\\d+(\\.\\d+)?");
			
			if(counter > 2) {
				
				if(counter == arrColKey.length)
					lastKey=eachKey;
					
				if(counter > 2 && counter!=arrColKey.length) {
					if(!numeric)
						concatColKey= concatColKey.concat("->").concat("'").concat(eachKey).concat("'");
					else {
						concatColKey= concatColKey.concat("->").concat(eachKey);
						numeric=false;
					}
				}
			}
		}
		
		if(instanceOfJson)
		{
			if(numeric)
				concatColKey= concatColKey.concat("->").concat(lastKey);	
			else
				concatColKey= concatColKey.concat("->").concat("'").concat(lastKey).concat("'");
		}
		else
		{

			if(numeric)
				concatColKey= concatColKey.concat("->>").concat(lastKey);	
			else
				concatColKey= concatColKey.concat("->>").concat("'").concat(lastKey).concat("'");
		}
		return concatColKey;
	}
	
	private boolean testStringIFInstanceOfJson(String jsonString) {
		
		Object convertedRequestObject = new JSONTokener(jsonString).nextValue();
		if (!(convertedRequestObject instanceof JSONObject || convertedRequestObject instanceof JSONArray))
			return false;
		return true;
	}
	
	public static WhereOperator validateOperator(String operator){
		
		WhereOperator ob= null;
		for (WhereOperator type : WhereOperator.values())
	        if (type.label.equalsIgnoreCase(operator))
	        	ob= type;	
		return ob;
	}
	
	public StringBuilder getQuery() {
		return query;
	}
}
