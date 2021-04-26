package com.hl.utilities.db.utils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;

public interface DBConstants {
	
	public static final String DB_ENDPOINT_DEFAULT 					= "DEFAULT";
	public static final String DB_ENDPOINT_SEPERATOR 				= ".";
	
	public static final String PROPERTY_KEY_DB_DEFAULT				= "DB.DEFAULT";
	public static final String PROPERTY_KEY_DB_TYPE		 			= "DB.TYPE";
	public static final String PROPERTY_KEY_DB_DRIVER		 		= "DB.DRIVER";
	public static final String PROPERTY_KEY_DB_URL			 		= "DB.URL";
	public static final String PROPERTY_KEY_DB_USERNAME		 		= "DB.USERNAME";
	public static final String PROPERTY_KEY_DB_PASSWORD		 		= "DB.PASSWORD";
	public static final String PROPERTY_KEY_DB_CONNECTION_TYPE		= "DB.CONNECTION.TYPE";
	public static final String PROPERTY_KEY_DBPOOL_INITIALSIZE 		= "DB.POOL.INITIAL.SIZE";
	public static final String PROPERTY_KEY_DBPOOL_MINIMUMSIZE 		= "DB.POOL.MINIMUM.SIZE";
	public static final String PROPERTY_KEY_DBPOOL_MAXSIZE		 	= "DB.POOL.MAX.SIZE";
	public static final String PROPERTY_KEY_DBPOOL_MAXIDLETIME 		= "DB.POOL.MAX.IDLE.TIME";
	public static final String DBCP2_URL= "jdbc:apache:commons:dbcp:dbcp-jcg-example";


    //Configuration parameters for the generation of the IAM Database Authentication token
   // private static final String RDS_INSTANCE_HOSTNAME = "ir-platform-db-postgres.cluster-cnomcnj4jcth.ap-south-1.rds.amazonaws.com";
   // private static final int RDS_INSTANCE_PORT = 5434;
   // private static final String JDBC_URL = "jdbc:postgresql://" + RDS_INSTANCE_HOSTNAME + ":" + RDS_INSTANCE_PORT+"/irdb";

    
}
