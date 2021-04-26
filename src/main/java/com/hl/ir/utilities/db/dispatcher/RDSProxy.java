package com.hl.ir.utilities.db.dispatcher;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import com.hl.ir.utilities.db.builder.DBConnectionDispatcherBuilder;
import com.hl.ir.utilities.properties.Client;
import com.hl.ir.utilities.properties.Properties;
import com.hl.ir.utilities.properties.builder.Type;
import com.hl.ir.utilities.properties.exception.PropertyNotFoundException;
import com.hl.utilities.db.utils.DBConstants;

public class RDSProxy implements DBConnectionDispatcher{

	private String dbUrl;
	private String username;
	private String AWS_ACCESS_KEY;
	private String AWS_SECRET_KEY;
	private Properties properties ;
	private String REGION_NAME = null;

	public RDSProxy() {}
	public RDSProxy(DBConnectionDispatcherBuilder builder) throws ClassNotFoundException, PropertyNotFoundException {
        
		properties = builder.getProperties();
		Class.forName(properties.getProperty(DBConstants.PROPERTY_KEY_DB_DRIVER));
	    DefaultAWSCredentialsProviderChain creds = new DefaultAWSCredentialsProviderChain();
	    username=properties.getProperty(DBConstants.PROPERTY_KEY_DB_USERNAME);
	    dbUrl= properties.getProperty(DBConstants.PROPERTY_KEY_DB_URL);
	    AWS_ACCESS_KEY = creds.getCredentials().getAWSAccessKeyId();
	    AWS_SECRET_KEY = creds.getCredentials().getAWSSecretKey();
	    REGION_NAME = Regions.AP_SOUTH_1.toString();
	}
	
	
	@Override
	public Connection getConnection() throws SQLException {
		
		System.out.println("In RDS Proxy getConnection()");
		Connection conn;
		try{
			conn= getDBConnectionUsingIam();
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("Failed to acquire db connection",e);
		}
		return  conn;

	}

	 /**
     * This method returns a connection to the db instance authenticated using IAM Database Authentication
     * @return
     * @throws Exception
     */
    private Connection getDBConnectionUsingIam() throws Exception {
    	//dbUrl= "";
        return DriverManager.getConnection(dbUrl, setMySqlConnectionProperties());
    }

    /**
     * This method sets the mysql connection properties which includes the IAM Database Authentication token
     * as the password. It also specifies that SSL verification is required.
     * @return
     * @throws PropertyNotFoundException 
     */
    private java.util.Properties setMySqlConnectionProperties() throws PropertyNotFoundException {
    	
    	java.util.Properties mysqlConnectionProperties = new java.util.Properties();
        mysqlConnectionProperties.setProperty("verifyServerCertificate","false");
        mysqlConnectionProperties.setProperty("useSSL", "false");
        mysqlConnectionProperties.setProperty("user","ir_dev");
        mysqlConnectionProperties.setProperty("password","ir_dev");
        return mysqlConnectionProperties;
    }

    /**
     * This method generates the IAM Auth Token.
     * An example IAM Auth Token would look like follows:
     * btusi123.cmz7kenwo2ye.rds.cn-north-1.amazonaws.com.cn:3306/?Action=connect&DBUser=iamtestuser&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20171003T010726Z&X-Amz-SignedHeaders=host&X-Amz-Expires=899&X-Amz-Credential=AKIAPFXHGVDI5RNFO4AQ%2F20171003%2Fcn-north-1%2Frds-db%2Faws4_request&X-Amz-Signature=f9f45ef96c1f770cdad11a53e33ffa4c3730bc03fdee820cfdf1322eed15483b
     * @return
     */
    private String generateAuthToken() {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY);

        RdsIamAuthTokenGenerator generator = RdsIamAuthTokenGenerator.builder()
                .credentials(new AWSStaticCredentialsProvider(awsCredentials)).region(REGION_NAME).build();
        return generator.getAuthToken(GetIamAuthTokenRequest.builder()
                .hostname("ir-platform-db-postgres.cluster-cnomcnj4jcth.ap-south-1.rds.amazonaws.com").port(5434).userName(username).build());
    }

}
