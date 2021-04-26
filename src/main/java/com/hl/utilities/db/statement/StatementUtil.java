package com.hl.utilities.db.statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.sql.rowset.CachedRowSet;

import com.hl.utilities.db.DBManager;
import com.hl.utilities.db.statement.exception.InvalidStatementException;
import com.sun.rowset.CachedRowSetImpl;
@Deprecated
public class StatementUtil {
	
	public static final String COLUMN_UNIQUE_ID = "unique_id";
	public static final String COLUMN_ENTITYID = "entityid";
	public static final String COLUMN_ENTITY_ID = "entity_id";
	
	public static Map<String,Object> selectOne(Statement statement) throws SQLException{
		return selectOne(null, statement);
	}
	public static Map<String,Object> selectOne(Connection conn, Statement statement) throws SQLException{
		if(statement!= null )
			statement.limit(1);
		List<Map<String,Object>> rows = select(conn, statement);
		if(rows.isEmpty())
			rows.add(new HashMap<>());
		return rows.get(0);
	}
	public static List<Map<String,Object>> select(Statement statement) throws SQLException{
		return select(null, statement);
	}
	public static List<Map<String,Object>> select(Connection connection, Statement statement) throws SQLException{
		if(statement.getTableName() == null || statement.getTableName().isEmpty())
			throw new SQLException("Empty Table Name");
		if(statement.getStatementType() != StatementType.SELECT)
			throw new InvalidStatementException("Incorrect query attempt");
		
		boolean releaseConnection = false;
		List<Map<String,Object>> rows = new ArrayList<>();
		String query = statement.toString();
		if(connection == null) {
			connection = DBManager.getInstance().getConnection();
			releaseConnection = true;
		}
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			preparedStatement = connection.prepareStatement(query.toString());
			resultSet = preparedStatement.executeQuery();
			rows = listForResultSet(resultSet);
		}
		finally {
			if(resultSet != null)
				resultSet.close();
			if(preparedStatement != null)
				preparedStatement.close();
			if(releaseConnection && connection != null)
				connection.close();
		}
		return rows;
	}
	
	private static List<Map<String,Object>> listForResultSet(ResultSet resultSet) throws SQLException{
		List<Map<String,Object>> rows = new ArrayList<>();
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columns = metaData.getColumnCount();
		while (resultSet.next()) {
			Map<String,Object> row = new HashMap<>(columns);
			for( int columnNumber = 1 ; columnNumber <= columns ; columnNumber++ ){           
				row.put(metaData.getColumnLabel(columnNumber), resultSet.getObject(columnNumber));
			}
			rows.add(row);
		}
		return rows;
	}
	
	public static int insert(List<Statement> statements) throws SQLException {
		return insert(null, statements);
	}
	public static int insert(Connection connection, List<Statement> statements) throws SQLException {
		StringBuilder errors = new StringBuilder();
		int rowsAffected = 0;
		for (Statement statement : statements) {
			try {
				insert(connection, statement);
				rowsAffected++;
			}
			catch(SQLException e) {
				errors.append(e.getMessage());
			}
		}
		if(errors.length() > 0)
			throw new SQLException(errors.toString());
		return rowsAffected;
	}
	public static int insert(Statement statement) throws SQLException {
		return insert(null, statement);
	}	
	public static int insert(Connection connection, Statement statement) throws SQLException {
		if(statement.getTableName() == null || statement.getTableName().isEmpty() || statement.getColumnValueMap() == null)
			throw new SQLException("Empty Table Name or Column Value Mapping");
		if(statement.getStatementType() != StatementType.INSERT)
			throw new InvalidStatementException("Incorrect query attempt");
		
		boolean releaseConnection = false;
		int id = -1;
		if(statement.getUniqueColumnName() != null && !statement.getUniqueColumnName().isEmpty()) {
			id = getNextValue(connection, statement.getTableName(), statement.getUniqueColumnName());
			statement.getColumnValueMap().put(statement.getUniqueColumnName(), id);
		}
		String query = statement.toString();
		if(connection == null) {
			connection = DBManager.getInstance().getConnection();
			releaseConnection = true;
		}
		try(PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
			preparedStatement.executeUpdate();
		}
		finally {
			if(releaseConnection && connection != null)
				connection.close();
		}
		return id;
	}
	
	public static int update(Statement statement) throws SQLException {
		return update(null,statement);
	}
	public static int update(Connection connection, Statement statement) throws SQLException {
		if(statement.getTableName() == null || statement.getTableName().isEmpty() || statement.getColumnValueMap() == null)
			throw new SQLException("Empty Table Name or Column Value Mapping");
		if(statement.getStatementType() != StatementType.UPDATE)
			throw new InvalidStatementException("Incorrect query attempt");
		
		boolean releaseConnection = false;
		
		int rowsAffected = -1;
		
		String query = statement.toString();
		if(connection == null) {
			connection = DBManager.getInstance().getConnection();
			releaseConnection = true;
		}
		try(PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
			rowsAffected = preparedStatement.executeUpdate();
		}
		finally {
			if(releaseConnection && connection != null)
				connection.close();
		}
		return rowsAffected;
	}
	
	public static int getNextValue(String tableName, String columnName) throws SQLException {
		return getNextValue(null, tableName, columnName);
	}
	public static int getNextValue(Connection connection, String tableName, String columnName) throws SQLException {
        int nextval= -1;
        if(tableName == null || tableName.isEmpty() || columnName == null || columnName.isEmpty())
            return nextval;
        String sequenceName = tableName.toLowerCase() + "_eid";
        if(!columnName.equalsIgnoreCase(COLUMN_ENTITYID) && !columnName.equalsIgnoreCase(COLUMN_ENTITY_ID))
            sequenceName= sequenceName.replace("_eid", "_uid");
        return getNextValueFromSequence(connection, sequenceName);
    }
	
	public static int getNextValueFromSequence(String sequenceName) throws SQLException {
		return getNextValueFromSequence(null, sequenceName);
	}
	public static int getNextValueFromSequence(Connection connection, String sequenceName) throws SQLException {
        int nextval= -1;
        if(sequenceName == null || sequenceName.isEmpty())
            return nextval;
        String query= "select nextval('"+sequenceName+"') as nextval";
//		TEMPORARY CHANGE FOR INCORPORATING MULTITENANCY PHASE 1 CHANGES OF DB UTIL (Using simple PreparedStatement for sequence query)
//        DBManager dbManager = connection == null ? DBManager.getInstance():DBManager.getInstance(false);
//        try(ResultSet resultSet = dbManager.select(connection, query)) {
//            resultSet.next();
//            nextval= resultSet.getInt("nextval");
//        }
        CachedRowSet rowSet =  new CachedRowSetImpl();
        PreparedStatement pst;
		pst = connection.prepareStatement(query.toString());
		rowSet.populate(pst.executeQuery());
		rowSet.next();
		nextval= rowSet.getInt("nextval");
        return nextval;
    }
	
	public static String sqlString(String value) {
        if (value != null)
        {
            StringBuilder sqlString = new StringBuilder();
            sqlString.append('\'');
            int from = 0;
            int next;
            while ((next = value.indexOf('\'', from)) != -1)
            {
                sqlString.append(value.substring(from, next + 1));
                sqlString.append('\'');
                from = next + 1;
            }
            if (from < value.length())
                sqlString.append(value.substring(from));
            sqlString.append('\'');
            return sqlString.toString().trim();
        }
        return null;
    }
	
	public static String sqlAlias(String alias) {
        if (alias != null && !alias.trim().isEmpty()) {
            StringBuilder sqlString = new StringBuilder();
            sqlString.append('\"');
            int from = 0;
            int next;
            while ((next = alias.indexOf('\"', from)) != -1)
            {
                sqlString.append(alias.substring(from, next + 1));
                sqlString.append('\"');
                from = next + 1;
            }
            if (from < alias.length())
                sqlString.append(alias.substring(from));
            sqlString.append('\"');
            return sqlString.toString().trim();
        }
        return "";
    }
	
	public static String now() {
		return sqlTimeStamp(Calendar.getInstance().getTime());
	}
    
	public static String sqlTimeStamp(Date date) {
		String posgresDefaultDateFormat = "yyyy-MM-dd HH:mm:ss";
		if(date == null)
            date = Calendar.getInstance().getTime();
		DateFormat dateFormatter = new SimpleDateFormat(posgresDefaultDateFormat);
		String dateString = dateFormatter.format(date) + " " + TimeZone.getDefault().getID();
		return dateString;	
    }
	
	public static String sqlTimeStamp(String dateString) throws ParseException {
		return sqlTimeStamp(dateString,null);	
    }
	
	public static String sqlTimeStamp(String dateString, String dateFormat) throws ParseException {
		DateFormat dateFormatter = null; 
		if(dateFormat == null || dateFormat.isEmpty())
			dateFormatter = new SimpleDateFormat();
		else
			dateFormatter = new SimpleDateFormat(dateFormat);
		Date date = dateFormatter.parse(dateString);
		return sqlTimeStamp(date);	
    }
	
	public static String sqlJoinAlias(String tableName, String columnName) {
		return new StringBuilder().append(tableName).append(".").append(columnName).toString();
	}
	
	public static Map<String,Object> insertUpdateConflictWithReturn(Connection connection, Statement insert, Statement update, 
			String targetValue, Statement select, UpsertConflictTargetType conflictTargetType) throws SQLException {
		
		int id = -1;
		if(insert.getUniqueColumnName() != null && !insert.getUniqueColumnName().isEmpty()) {
			id = getNextValue(insert.getTableName(), insert.getUniqueColumnName());
			insert.getColumnValueMap().put(insert.getUniqueColumnName(), id);
		}
		
		StringBuilder query = new StringBuilder();
		query.append(insert.toString());
		query.append(" ON CONFLICT ");
		if(conflictTargetType.equals(UpsertConflictTargetType.COLUMNS)) {
			query.append(" (").append(targetValue).append(") ");
		}else if(conflictTargetType.equals(UpsertConflictTargetType.CONSTRAINT)) {
			query.append(" ON CONSTRAINT ").append(targetValue);
		}else if(conflictTargetType.equals(UpsertConflictTargetType.WHERE_PREDICATE)) {
			query.append(" ON CONFLICT ").append(targetValue);
		}
		
		query.append(" DO ");		
		if(update == null) {
			query.append(" NOTHING ");
		}else{
			query.append(update.toString());
		}
		query.append(" returning ");
		List<String> selectColumns = select.getSelectColumns();
		for (int i=0;i<selectColumns.size();i++) {
			query.append(selectColumns.get(i));
			if(i < selectColumns.size() - 1) {
				query.append(",");
			}
		}
		
	    boolean releaseConnection = false;
        
        if(connection == null) {
            connection = DBManager.getInstance().getConnection();
            releaseConnection = true;
        }
        ResultSet resultSet = null;
		Map<String,Object> row = new HashMap<>();
        try(PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
        	boolean executed = preparedStatement.execute();
        	resultSet = preparedStatement.getResultSet();
            List<Map<String,Object>> rows = new ArrayList<>();			
			rows = listForResultSet(resultSet);
			if(rows!=null && !rows.isEmpty()) {
				row = rows.get(0);
				row.put(insert.getUniqueColumnName(), id);//putting unique column
			}
        }
        finally {
            if(releaseConnection && connection != null)
                connection.close();
        }
        return row;

	}
	
	public static int delete(Statement statement) throws SQLException {
		return delete(null,statement);
	}
	public static int delete(Connection connection, Statement statement) throws SQLException {
		if(statement.getTableName() == null || statement.getTableName().isEmpty())
			throw new SQLException("Blank Table Name ");
		if(statement.getStatementType() != StatementType.DELETE)
			throw new InvalidStatementException("Incorrect query attempt");
		
		boolean releaseConnection = false;
		
		int rowsAffected = -1;
		
		String query = statement.toString();
		if(connection == null) {
			connection = DBManager.getInstance().getConnection();
			releaseConnection = true;
		}
		try(PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
			rowsAffected = preparedStatement.executeUpdate();
		}
		finally {
			if(releaseConnection && connection != null)
				connection.close();
		}
		return rowsAffected;
	}
}
