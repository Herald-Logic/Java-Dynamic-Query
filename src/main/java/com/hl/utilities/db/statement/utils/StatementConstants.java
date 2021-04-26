package com.hl.utilities.db.statement.utils;

@Deprecated
public interface StatementConstants {

	public static final String STRING_SEPERATOR = "|$|";
	public static final String STRING_SPLITTER = "\\|\\$\\|";
	
	public static final String QUERY_KEYWORD_DISTINCT = " distinct ";
	public static final String QUERY_KEYWORD_OR = " OR ";
	public static final String QUERY_KEYWORD_AND = " AND ";
	public static final String QUERY_KEYWORD_SELECT = " SELECT ";
	public static final String QUERY_KEYWORD_FROM = " FROM ";
	public static final String QUERY_KEYWORD_WHERE = " WHERE ";
	public static final String QUERY_KEYWORD_ORDER_BY = " ORDER BY ";
	public static final String QUERY_KEYWORD_ORDER_BY_DESC = " DESC ";
	public static final String QUERY_KEYWORD_LIMIT = " LIMIT ";
	public static final String QUERY_KEYWORD_OFFSET = " OFFSET ";
	public static final String QUERY_DUMMY_CONDITTION_AND = " 1 = 1 ";
	public static final String QUERY_DUMMY_CONDITION_OR = " 1 <> 1 ";
}
