package com.hl.utilities.db.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import javax.sql.rowset.CachedRowSet;
import com.sun.rowset.CachedRowSetImpl;

public class DBUtil {

	public static final String COLUMN_ENTITY_ID = "entity_id";
	public static final String COLUMN_ENTITYID = "entityid";

	public static String sqlString(Object obj) {
		if (obj != null)
		{
			String c= obj.toString();
			StringBuilder sqlString = new StringBuilder();
			sqlString.append('\'');
			int from = 0;
			int next;
			while ((next = c.indexOf('\'', from)) != -1)
			{
				sqlString.append(c.substring(from, next + 1));
				sqlString.append('\'');
				from = next + 1;
			}
			if (from < c.length())
				sqlString.append(c.substring(from));
			sqlString.append('\'');
			return sqlString.toString().trim();
		}
		return null;
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
	
	public static int getNextValue(String tableName, String columnName) throws SQLException {
		return getNextValue(null, tableName, columnName);
	}
	public static int getNextValue(Connection connection, String tableName, String columnName) throws SQLException {
        int nextval= -1;
        if(tableName == null || tableName.isEmpty() || columnName == null || columnName.isEmpty())
            return nextval;
        String sequenceName = tableName.toLowerCase() + "_eid";
        //if(!columnName.equalsIgnoreCase(COLUMN_ENTITYID) && !columnName.equalsIgnoreCase(COLUMN_ENTITY_ID))
           // sequenceName= sequenceName.replace("_eid", "_uid");
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
	
}
