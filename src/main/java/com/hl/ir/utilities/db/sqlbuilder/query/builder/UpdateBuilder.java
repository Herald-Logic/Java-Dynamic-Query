package com.hl.ir.utilities.db.sqlbuilder.query.builder;

import java.util.HashMap;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hl.ir.utilities.db.sqlbuilder.query.Update;
import com.hl.ir.utilities.db.sqlbuilder.query.Where;
import com.hl.ir.utilities.db.sqlbuilder.query.exception.QueryException;
import com.hl.ir.utilities.db.sqlbuilder.query.impl.PostgresUpdate;
import com.hl.utilities.db.DBType;

public class UpdateBuilder implements Builder<Update> {

	private Map<String, Object> updateMap;
	private Map<String, JsonObject> jsonArrayMap;
	private DBType dbType;
	private String table;
	private String where;

	{
		updateMap = new HashMap<String, Object>();
		jsonArrayMap = new HashMap<String, JsonObject>();
	}

	@Override
	public Update build() throws QueryException {
		switch(this.dbType) {
		case POSTGRES: return new PostgresUpdate(this);
		default: return null;
		}
	}

	public UpdateBuilder dbType(DBType type) {
		this.dbType = type;
		return this;
	}
	
	public UpdateBuilder table(String table) {
		this.table = table;
		return this;
	}
	
	public UpdateBuilder where(Where where) {
		this.where = " where "+where.fetch();
		return this;
	}

	public UpdateBuilder set(String key, Object value) throws Exception {
		return set(key, value, "action:upsert");
	}

	public UpdateBuilder set(String key, Object value, JsonObject condition) throws Exception {
		return set(key, value, condition, "action:upsert");
	}	

	public UpdateBuilder set(String key, Object value, String action) throws Exception {
		if(!key.contains(".")) {
			updateMap.put(key, value);
		} else {
			String[] keys = key.split("[.]");
			String column = keys[0];
			if(!key.contains("#")) {
				JsonObject jsonObject = formJsonObject(keys, value);
				if(updateMap.containsKey(column)) {
					JsonObject presentObject = (JsonObject) updateMap.get(column);
					copyJsonObjects(presentObject, jsonObject);
				}
				updateMap.put(column, jsonObject);
				updateMap.forEach((k,v)->{
					System.out.println(k+" : "+v);
				});
			} else {
				if(keys.length > 2)
					throw new Exception("Cannot have # for JsonObjects Array");
				else {
					JsonArray jsonArray = new JsonArray();
					jsonArray.add(action);
					if(updateMap.containsKey(column))
						jsonArray = (JsonArray) updateMap.get(column);
					jsonArray.add((String) value);
					updateMap.put(column, jsonArray);
				}
			}
		}
		return this;
	}

	public UpdateBuilder set(String key, Object value, JsonObject condition, String action) throws Exception {
		//currently, entire column has to be an array.. a key with value as array is not supported
		String[] keys = key.split("[.]");
		String conditionKey = keys[0]+"#";
		conditionKey += new Gson().toJson(condition);
		JsonObject jsonObject = formJsonObject(keys, value);
		if(jsonArrayMap.containsKey(conditionKey)) {
			JsonObject presentObject = jsonArrayMap.get(conditionKey);
			copyJsonObjects(presentObject, jsonObject);
		}
		copyJsonObjects(condition, jsonObject);
		JsonArray uniqueIds = new JsonArray();
		for(String uniqueId : condition.keySet())
			uniqueIds.add(uniqueId);
		jsonObject.add("unique_ids", uniqueIds);
		jsonArrayMap.put(conditionKey, jsonObject);
		JsonArray jsonArray = new JsonArray();
		jsonArray.add(action);
		for(String k: jsonArrayMap.keySet()) {
			if(k.contains(keys[0]))
				jsonArray.add(jsonArrayMap.get(k));
		}
		updateMap.put(keys[0], jsonArray);
		return this;
	}

	private JsonObject formJsonObject(String[] keys,Object value) {
		JsonObject jsonObject = new JsonObject();
		if(value == null)
			jsonObject.add(keys[keys.length-1], null);
		else if(value instanceof Integer || value instanceof Float || value instanceof Double || value instanceof Long || value instanceof Number)
			jsonObject.addProperty(keys[keys.length-1], (Number)value);
		else if(value instanceof String)
			jsonObject.addProperty(keys[keys.length-1], (String)value);
		else if(value instanceof Boolean)
			jsonObject.addProperty(keys[keys.length-1], (Boolean)value);
		for(int i=keys.length-2; i>0; i--) {
			if(!keys[i].equals("#")) {
				JsonObject tempObject = new JsonObject();
				tempObject.add(keys[i], jsonObject);
				jsonObject = tempObject;
			}
		}
		return jsonObject;
	}

	private void copyJsonObjects(JsonObject oldJson, JsonObject mergeJson) {
		for(String key: oldJson.keySet()) {
			if(mergeJson.keySet().contains(key)) {
				if(mergeJson.get(key).isJsonObject())
					copyJsonObjects(oldJson.get(key).getAsJsonObject(), mergeJson.get(key).getAsJsonObject());
				else if(mergeJson.get(key).isJsonArray()) {
					if(oldJson.get(key).isJsonArray()) {
						for(JsonElement jsonElement: oldJson.get(key).getAsJsonArray())
							mergeJson.get(key).getAsJsonArray().add(jsonElement);
					}
				}
				else 
					mergeJson.add(key, oldJson.get(key));
			}
			else 
				mergeJson.add(key, oldJson.get(key));
		}
	}

	public Map<String, Object> getUpdateMap() {
		return updateMap;
	}

	public String getTable() {
		return table;
	}

	public String getWhere() {
		return where;
	}

}
