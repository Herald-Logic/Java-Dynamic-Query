package com.hl.utilities.db.statement;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import com.hl.ir.utilities.properties.Client;
import com.hl.ir.utilities.properties.Properties;
import com.hl.utilities.db.DBManager;
import com.hl.utilities.db.DBType;
import com.hl.utilities.db.statement.exception.InvalidStatementException;
import com.hl.utilities.db.statement.utils.StatementConstants;

@Deprecated
public abstract class Statement {

	private final StatementType statementType;
	private String tableName;
	private String uniqueColumnName;
	private int limit;
	private int offset;
	private List<String> selectColumns;

	private List<String> joinColumns;
	private Map<String, Object> columnValueMap;
	private List<String> whereColumns;
	private Map<String, Boolean> orderByColumns;
	private Map<String, Object> uniqueIdsMap;

	{
		this.selectColumns = new ArrayList<>();
		this.columnValueMap = new HashMap<>();
		this.whereColumns = new ArrayList<>();
		this.joinColumns = new ArrayList<>();
		this.orderByColumns = new LinkedHashMap<>();
		this.limit = -1;
		this.offset = -1;
		this.uniqueIdsMap = new HashMap<>();
	}

	protected Statement(StatementType statementType) {
		this.statementType = statementType;
	}
	protected Statement(StatementType statementType, String tableName) {
		this.statementType = statementType;
		this.tableName = tableName;
	}

	public StatementType getStatementType() {
		return statementType;
	}
	public String getTableName() {
		return tableName;
	}
	public Statement setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	} 
	public String getUniqueColumnName() {
		return uniqueColumnName;
	}
	public Statement setUniqueColumnName(String uniqueColumnName) {
		this.uniqueColumnName = uniqueColumnName;
		return this;
	}
	public List<String> getSelectColumns() {
		return selectColumns;
	}
	public List<String> getJoinColumns() {
		return joinColumns;
	}
	public Map<String, Object> getColumnValueMap() {
		return columnValueMap;
	}
	public List<String> getWhereColumns() {
		return whereColumns;
	}
	public Map<String,Boolean> getOrderByColumns() {
		return orderByColumns;
	}
	public int getLimit() {
		return limit;
	}
	
	public int getOffset() {
		return offset;
	}
	public Statement select(String columnName) {
		select(columnName, null);
		return this;
	}
	public Statement select(String columnName, String alias) {
		this.selectColumns.add(columnName + " " + StatementUtil.sqlAlias(alias));
		return this;
	}
	public Statement insert(String columnName, Object columnValue) {
		return insert(columnName, columnValue, true);
	}
	public Statement insert(String columnName, Object columnValue, boolean constructSQLString) {
		if(this.getStatementType() != StatementType.INSERT)
			throw new InvalidStatementException("Insert is not supported for this Statement");
		if (columnValue instanceof java.util.Date || columnValue instanceof Date || columnValue instanceof Timestamp)
			columnValue = StatementUtil.sqlTimeStamp((java.util.Date)columnValue);
		if(columnValue instanceof String && constructSQLString)
			columnValue = StatementUtil.sqlString(columnValue.toString());
		columnValueMap.put(columnName, columnValue);
		return this;
	}
	public Statement set(String columnName, Object columnValue) {
		return set(columnName, columnValue, true);
	}
	public Statement set(String columnName, Object columnValue, boolean constructSQLString) {
		if(this.getStatementType() != StatementType.UPDATE)
			throw new InvalidStatementException("Set is not supported for this Statement");
		if (columnValue instanceof java.util.Date || columnValue instanceof Date || columnValue instanceof Timestamp)
			columnValue = StatementUtil.sqlTimeStamp((java.util.Date)columnValue);
		if(columnValue instanceof String && constructSQLString)
			columnValue = StatementUtil.sqlString(columnValue.toString());
		columnValueMap.put(columnName, columnValue);
		return this;
	}
	public Statement join(String firstColumn, String secondColumn) {
		return join("=", firstColumn, secondColumn);
	}
	public Statement join(String operator, String firstColumn, String secondColumn) {
		if(this.getStatementType() != StatementType.SELECT)
			throw new InvalidStatementException("Join is not supported for this Statement");
		this.joinColumns.add(firstColumn + StatementConstants.STRING_SEPERATOR + operator + StatementConstants.STRING_SEPERATOR + secondColumn);
		return this;
	}

	public Statement resetWhere() {
		this.whereColumns.clear();
		return this;
	}
	public Statement where(String queryString) {
		this.where(queryString, "(", ")");
		return this;
	}
	public Statement where(String columnName, Object columnValue) {
		return this.where("=", columnName, columnValue);
	}
	public Statement where(String operator, String columnName, Object columnValue) {
		if(this.getStatementType() != StatementType.SELECT && this.getStatementType() != StatementType.UPDATE && this.getStatementType() != StatementType.DELETE)
			throw new InvalidStatementException("Where is not supported for this Statement");
		if(columnValue == null) {
			operator = "is";
		}
		else if(!columnValue.equals(")") && (columnValue instanceof String || columnValue instanceof Date || columnValue instanceof Timestamp))
			columnValue = StatementUtil.sqlString(columnValue.toString());
		this.whereColumns.add(columnName + StatementConstants.STRING_SEPERATOR + operator + StatementConstants.STRING_SEPERATOR + columnValue);
		return this;
	}
	public Statement orderBy(String columnName, boolean isDesc) {
		if(this.getStatementType() != StatementType.SELECT)
			throw new InvalidStatementException("Order By is not supported for this Statement");
		this.orderByColumns.put(columnName, isDesc);
		return this;
	}
	public Statement limit(int limit) {
		if(this.getStatementType() != StatementType.SELECT)
			throw new InvalidStatementException("Limit is not supported for this Statement");
		this.limit = limit;
		return this;
	}

	public Statement offset(int offset) {
		if(this.getStatementType() != StatementType.SELECT)
			throw new InvalidStatementException("Offset is not supported for this Statement");
		this.offset = offset;
		return this;
	}
	
	@Override
	public String toString() {
		if(this.getStatementType() == StatementType.INSERT)
			return insertQuery();
		else if(this.getStatementType() == StatementType.UPDATE)
			return updateQuery();
		else if(this.getStatementType() == StatementType.SELECT)
			return selectQuery();
		else if(this.getStatementType() == StatementType.DELETE)
			return deleteQuery();
		return super.toString();
	}

	private String insertQuery() {
		StringBuilder query = new StringBuilder();
		if(!this.getColumnValueMap().isEmpty()) {
			query.append("INSERT INTO ");
			query.append(this.getTableName());

			StringBuilder columnNames = new StringBuilder(" ");
			StringBuilder values = new StringBuilder(" ");
			for (Map.Entry<String, Object> columnValueEntry : this.getColumnValueMap().entrySet()) {
				columnNames.append(columnValueEntry.getKey() + ",");
				values.append(columnValueEntry.getValue()).append(",");
			}
			query.append("(" +columnNames.substring(0, columnNames.lastIndexOf(","))  +")");
			query.append(" VALUES (" + values.substring(0, values.lastIndexOf(",")) + ")");
		}
		return query.toString();
	}
	private String updateQuery() {
		//DBType dbType = DBManager.getInstance().getDbType();
		DBType dbType=DBType.POSTGRES;
		StringBuilder query = new StringBuilder();
		List<String> jsonbs= new ArrayList<String>();
		if(!this.getColumnValueMap().isEmpty()) {
			query.append("UPDATE ").append(this.getTableName());
			query.append(" SET ");
			if(dbType == DBType.POSTGRES) 
				getListOfJsonBColumns(jsonbs);
			for (Map.Entry<String, Object> setEntry : this.getColumnValueMap().entrySet()) {
				if(dbType == DBType.POSTGRES && jsonbs.contains(setEntry.getKey())) {
					if(this.uniqueIdsMap.get(setEntry.getKey()) != null) {
						//DISCLAIMER : UPDATION OF JSONOBJECT IN JSONARRAY IS SUPPORTED ONLY FOR ONE OBJECT IN ONE QUERY
						String arrayActionSpec= "update";
						if(((JSONObject)this.uniqueIdsMap.get(setEntry.getKey())).has("array-action-spec"))
							arrayActionSpec= ((JSONObject)this.uniqueIdsMap.get(setEntry.getKey())).getString("array-action-spec");
						if("update".equals(arrayActionSpec)) {
							String value= ((String) setEntry.getValue()).replaceAll("'", "");
							if(value.startsWith("["))
								value = value.substring(1,  value.length()-1);
							if(value.startsWith("\"")) {
								String coalesce= "coalesce ("+setEntry.getKey()+", '{}'::jsonb)";
								String[] strings= value.split(",");
								for(int k=0; k<strings.length; k++) {
									int index= getStringIndex(setEntry.getKey(), strings[k].trim());
									if(index==999)
										coalesce +=	" || ('"+strings[k].trim()+"')::jsonb";
								}
								query.append(setEntry.getKey()).append(" = ").append(coalesce).append(",");		
							}
							else {
								JSONObject valueJson= new JSONObject(value);
								int index= getIndexForJsonArray(setEntry.getKey(), valueJson);
								if(index==999) {
									String coalesce= "coalesce ("+setEntry.getKey()+", '{}'::jsonb)";
									coalesce +=	" || ('"+value+"')::jsonb";
									query.append(setEntry.getKey()).append(" = ").append(coalesce).append(",");		
								}
								else {
									//DISCLAIMER : UPDATION OF JSONOBJECT IN JSONARRAY IS SUPPORTED ONLY FOR ONE OBJECT IN ONE QUERY
									String coalesce= "jsonb_set("+setEntry.getKey()+" , '{"+index+"}', "+setEntry.getKey()+"->"+index+" ";
									for(String key: valueJson.keySet()) {
										if(valueJson.get(key).toString() != null && valueJson.get(key).toString().equals(""))
											coalesce += " || ('{\""+key+"\": \"\"}')::jsonb";
										else if(valueJson.get(key).toString().charAt(0) == '{' || valueJson.get(key).toString().charAt(0) == '[') 
											coalesce +=	" || ('{\""+key+"\": "+valueJson.get(key).toString()+"}')::jsonb";
										else {
											if (this.uniqueIdsMap != null) {
												JSONObject uniqMap= ((JSONObject)this.uniqueIdsMap.get(setEntry.getKey()));

												if ((uniqMap != null) && (uniqMap.has("unique_id")) && ("number".equalsIgnoreCase((String) ((JSONObject) uniqMap.get("unique_id")).get("type")))) {
													coalesce +=	" || ('{\""+key+"\": "+valueJson.get(key).toString()+"}')::jsonb";
												} else {
													coalesce +=	" || ('{\""+key+"\": \""+valueJson.get(key).toString()+"\"}')::jsonb";
												}
											} else {
												coalesce +=	" || ('{\""+key+"\": \""+valueJson.get(key).toString()+"\"}')::jsonb";
											}
										}
									}
									coalesce+= ")";
									query.append(setEntry.getKey()).append(" = ").append(coalesce).append(",");	
								}
							}
						}
						else {
							List<Integer> ordinality= new ArrayList<Integer>();
							List<Integer> queryOrdinality = new ArrayList<Integer>();
							String value= ((String) setEntry.getValue()).replaceAll("'", "");
							if(value.startsWith("["))
								value = value.substring(1,  value.length()-1);
							if(value.startsWith("\"")) {
								String coalesce= setEntry.getKey();
								String[] strings= value.split(",");
								for(int k=0; k<strings.length; k++) {
									int index= getStringIndex(setEntry.getKey(), strings[k].trim());
									if(index!=999)
										ordinality.add(index);
								}
								Collections.sort(ordinality);
								for(int ord=0; ord<ordinality.size(); ord++)
									queryOrdinality.add(ordinality.get(ord) - ord);
								for(int ord: queryOrdinality) 
									coalesce +=	" - "+ord;
								query.append(setEntry.getKey()).append(" = ").append(coalesce).append(",");		
							}
							else {
								//DISCLAIMER : UPDATION OF JSONOBJECT IN JSONARRAY IS SUPPORTED ONLY FOR ONE OBJECT IN ONE QUERY
								JSONObject valueJson= new JSONObject(value);
								int index= getIndexForJsonArray(setEntry.getKey(), valueJson);
								if(index!=999) {
									String coalesce= setEntry.getKey();
									coalesce +=	" - "+index;
									query.append(setEntry.getKey()).append(" = ").append(coalesce).append(",");		
								}
							}
						}
					}
					else {
						String coalesce= "coalesce ("+setEntry.getKey()+", '{}'::jsonb)";
						String value= ((String) setEntry.getValue()).replaceAll("'", "");
						if(value.startsWith("["))
							value = value.substring(1,  value.length()-1);
						JSONObject valueJson= new JSONObject(value);
						for(String key: valueJson.keySet()) {
							if(valueJson.get(key).toString() != null && valueJson.get(key).toString().equals(""))
								coalesce += " || ('{\""+key+"\": \"\"}')::jsonb";
							else if(valueJson.get(key).toString().charAt(0) == '{' || valueJson.get(key).toString().charAt(0) == '[') 
								coalesce +=	" || ('{\""+key+"\": "+valueJson.get(key).toString()+"}')::jsonb";
							else
								coalesce +=	" || ('{\""+key+"\": \""+valueJson.get(key).toString()+"\"}')::jsonb";
						}
						query.append(setEntry.getKey()).append(" = ").append(coalesce).append(",");	
					}
				}
				else
					query.append(setEntry.getKey()).append(" = ").append(setEntry.getValue()).append(",");
			}
			query = new StringBuilder(query.substring(0, query.lastIndexOf(",")));

			query.append(StatementConstants.QUERY_KEYWORD_WHERE).append(StatementConstants.QUERY_DUMMY_CONDITTION_AND);
			for (String whereString : this.getWhereColumns()) {
				String[] whereArray = whereString.split(StatementConstants.STRING_SPLITTER);
				query.append(StatementConstants.QUERY_KEYWORD_AND).append(whereArray[0]).append(" ").append(whereArray[1]).append(" ").append(whereArray[2]);
			}
		}
		return query.toString();
	}
	
	private int getStringIndex(String columnName, String value) {
		Connection connection= null;
		try {
			connection = DBManager.getInstance().getConnection();
			StringBuilder isJsonbQuery = new StringBuilder();
			isJsonbQuery.append("select ord-1 as pos from "+this.getTableName()+" r , jsonb_array_elements(r."+columnName+") WITH ordinality as arr(opt, ord) where opt = '"+value+"'");
			isJsonbQuery.append(" and ").append(StatementConstants.QUERY_DUMMY_CONDITTION_AND);
			for (String whereString : this.getWhereColumns()) {
				String[] whereArray = whereString.split(StatementConstants.STRING_SPLITTER);
				isJsonbQuery.append(StatementConstants.QUERY_KEYWORD_AND).append(whereArray[0]).append(" ").append(whereArray[1]).append(" ").append(whereArray[2]);
			}
			try(PreparedStatement pst= connection.prepareStatement(isJsonbQuery.toString())) {
				try (ResultSet resultSet= pst.executeQuery()) {
					while(resultSet.next()) {
						return resultSet.getInt("pos");
					}
				}catch(Exception e) {
					e.printStackTrace();
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return 999;
	}
	
	private int getIndexForJsonArray(Object columnName, Object value) {
		JSONObject uniqueIds= (JSONObject) this.uniqueIdsMap.get(columnName);
		JSONObject request= (JSONObject) value;
		String jsonArraywhereClause= "";
		for(String key: uniqueIds.keySet()) {
			if(!"array-action-spec".equals(key))
				jsonArraywhereClause += " opt ->> '"+key+"' = '"+request.get(key)+"' and";
		}
		jsonArraywhereClause = jsonArraywhereClause.substring(0, jsonArraywhereClause.length()-4);
		Connection connection= null;
		try {
			connection = DBManager.getInstance().getConnection();
			StringBuilder isJsonbQuery = new StringBuilder();
			isJsonbQuery.append("select ord-1 as pos from "+this.getTableName()+" r , jsonb_array_elements(r."+columnName+") WITH ordinality as arr(opt, ord) where "+jsonArraywhereClause);
			isJsonbQuery.append(" and ").append(StatementConstants.QUERY_DUMMY_CONDITTION_AND);
			for (String whereString : this.getWhereColumns()) {
				String[] whereArray = whereString.split(StatementConstants.STRING_SPLITTER);
				isJsonbQuery.append(StatementConstants.QUERY_KEYWORD_AND).append(whereArray[0]).append(" ").append(whereArray[1]).append(" ").append(whereArray[2]);
			}
			System.out.println("OPT:"+ isJsonbQuery.toString());
			try(PreparedStatement pst= connection.prepareStatement(isJsonbQuery.toString())) {
				try (ResultSet resultSet= pst.executeQuery()) {
					while(resultSet.next()) {
						return resultSet.getInt("pos");
					}
				}catch(Exception e) {
					e.printStackTrace();
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return 999;
	}
	private void getListOfJsonBColumns(List<String> jsonbs) {
		System.out.println("in getListOfJsonBColumns:{}");
		String colNames= "";
		for(String colName: this.columnValueMap.keySet()) {
			colNames+= "'"+colName+"' ,";
		}
		
		Properties properties= null;
		try {
			Client client= Client.builder().fetch();
			properties= Properties.builder().client(client).build();
			Class.forName("org.postgresql.Driver");  
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		Connection connection=null;
		try {
			String url= properties.getProperty("DB.URL");
			String username= properties.getProperty("DB.USERNAME");
			String password= properties.getProperty("DB.PASSWORD");;
			connection=DriverManager.getConnection(url,username,password); 		
			String isJsonbQuery= "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = '"+this.getTableName()+"' and COLUMN_NAME IN("+colNames.substring(0, colNames.length()-2)+")";
			try(PreparedStatement pst= connection.prepareStatement(isJsonbQuery)) {
				try (ResultSet resultSet= pst.executeQuery()) {
					while(resultSet.next()) {
						if("jsonb".equalsIgnoreCase(resultSet.getString("DATA_TYPE"))) {
							jsonbs.add(resultSet.getString("COLUMN_NAME"));
						}
					}
				}catch(Exception e) {
					e.printStackTrace();
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} 
		finally {
			if(connection !=null)
				try {connection.close();} catch (SQLException e) {e.printStackTrace();}
		}
		System.out.println("getListOfJsonBColumns:() SUCCESS");
	}
	
	private String selectQuery() {
		StringBuilder query = new StringBuilder();
		query.append(StatementConstants.QUERY_KEYWORD_SELECT);
		if(!this.getSelectColumns().isEmpty()) {
			for (String selectColumn : this.getSelectColumns()) {
				query.append(selectColumn).append(",");
			}
		}
		else {
			query.append(" *, ");
		}
		query = new StringBuilder(query.substring(0, query.lastIndexOf(",")))
				.append(StatementConstants.QUERY_KEYWORD_FROM).append(this.getTableName()).append(StatementConstants.QUERY_KEYWORD_WHERE)
				.append(StatementConstants.QUERY_DUMMY_CONDITTION_AND);
		for (String joinColumnString: this.getJoinColumns()) {
			String[] joinArray = joinColumnString.split(StatementConstants.STRING_SPLITTER); 
			query.append(StatementConstants.QUERY_KEYWORD_AND).append(joinArray[0]).append(" ").append(joinArray[1]).append(" ").append(joinArray[2]);
		}
		for (String whereString : this.getWhereColumns()) {
			String[] whereArray = whereString.split(StatementConstants.STRING_SPLITTER);
			query.append(StatementConstants.QUERY_KEYWORD_AND).append(whereArray[0]).append(" ").append(whereArray[1]).append(" ").append(whereArray[2]);
		}
		if(!this.getOrderByColumns().isEmpty()) {
			query.append(StatementConstants.QUERY_KEYWORD_ORDER_BY);
			for (Map.Entry<String, Boolean> orderByEntry : this.getOrderByColumns().entrySet()) {
				query.append(orderByEntry.getKey());
				if(orderByEntry.getValue())
					query.append(" ").append(StatementConstants.QUERY_KEYWORD_ORDER_BY_DESC).append(" ");
				query.append(" , ");
			}
			query = new StringBuilder(query.substring(0, query.lastIndexOf(",")));
		}
		if(this.getLimit() > 0) {
			query.append(" ").append(StatementConstants.QUERY_KEYWORD_LIMIT).append(" ").append(this.getLimit());
		}
		if(this.getOffset() > 0) {
			query.append(" ").append(StatementConstants.QUERY_KEYWORD_OFFSET).append(" ").append(this.getOffset());
		}

		return query.toString();
	}

	private String deleteQuery() {
		if(this.getWhereColumns().isEmpty()) {
			throw new InvalidStatementException("where clause in delete Statement cannot be empty");
		}

		StringBuilder query = new StringBuilder();
		query.append("DELETE FROM ").append(this.tableName);
		query.append(StatementConstants.QUERY_KEYWORD_WHERE).append(StatementConstants.QUERY_DUMMY_CONDITTION_AND);

		for (String whereString : this.getWhereColumns()) {
			String[] whereArray = whereString.split(StatementConstants.STRING_SPLITTER);
			query.append(StatementConstants.QUERY_KEYWORD_AND).append(whereArray[0]).append(" ").append(whereArray[1]).append(" ").append(whereArray[2]);
		}
		return query.toString();
	}

	public Statement setUniqueIds(String columnName, Object columnValue) {
		uniqueIdsMap.put(columnName, columnValue);
		return this;
	}
}
