package com.hl.ir.utilities.db.test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import com.hl.ir.utilities.db.builder.DBConnectionDispatcherBuilder;
import com.hl.ir.utilities.db.dispatcher.DBConnectionDispatcher;
import com.hl.ir.utilities.db.dispatcher.PooledDBConnection;
import com.hl.ir.utilities.db.dispatcher.SimpleDBConnection;
import com.hl.ir.utilities.properties.Client;
import com.hl.ir.utilities.properties.Properties;
import com.hl.ir.utilities.properties.builder.ClientBuilder;
import com.hl.ir.utilities.properties.builder.PropertiesBuilder;
import com.hl.ir.utilities.properties.builder.Type;
import com.hl.ir.utilities.properties.exception.PropertyNotFoundException;

class DBConnectionDispatcherBuilderTest {
	
	@InjectMocks
	private  DBConnectionDispatcherBuilder builder ;
	private static Connection connection = null;	
	
	@Mock
	ClientBuilder clientBuilder;
	@Mock
	Client client;
	@Mock
	DBConnectionDispatcher dbConnectionDispatcher;
	
	@BeforeEach
	public void init() {
		MockitoAnnotations.initMocks(this);
	}

	@AfterEach
	public void afterAll() throws SQLException {

		if(connection !=null) {
			System.out.println("Closing connection");
			connection.rollback();
			connection.close();
		}
	}
	
	@Test
	void testServiceNotFound() throws PropertyNotFoundException {
		
		try {
			Client.builder().domain("wlh").service("object_definition_read")	
			.client("hdfc")
			.build();
			Client client1= Client.builder().fetch();
			Properties prop= Properties.builder().build();
			when(Properties.builder().client(client1).type(Type.DYNAMO).build()).thenReturn(prop);
			
			dbConnectionDispatcher= builder.build();
			System.out.println(dbConnectionDispatcher.getConnection());
		}
		//catch (PropertyNotFoundException pnf) {
			//pnf.printStackTrace();
		//}
		catch (Exception ex) {
			ex.printStackTrace();
		}
		
		//when(builder.getExtendableDAOs(null, "APP")).thenReturn(objectListInput);
	}

	@Test
	public void testSimpleDBConnection() throws PropertyNotFoundException, ClassNotFoundException, SQLException
	{
		SimpleDBConnection dispatcherConn= new SimpleDBConnection();
		dispatcherConn.setDbUrl("jdbc:postgresql://ir-platform-db-postgres.cluster-cnomcnj4jcth.ap-south-1.rds.amazonaws.com:5434/irdb");
		dispatcherConn.setUsername("ir_dev");
		dispatcherConn.setPassword("ir_dev");
		connection=dispatcherConn.getConnection();
		System.out.println(connection);
		connection.setAutoCommit(false);
	}
	
	//@Test
	public void testPooledDBConnection() throws PropertyNotFoundException, ClassNotFoundException, SQLException
	{
		//Client.builder().domain("wlh").service("object_definition_read")	
		//.client("hdfc")
		//.build();
		
		PooledDBConnection dispatcherConn= new PooledDBConnection();
		dispatcherConn.setDbUrl("jdbc:postgresql://ir-platform-db-postgres.cluster-cnomcnj4jcth.ap-south-1.rds.amazonaws.com:5434/irdb");
		dispatcherConn.setUsername("ir_dev");
		dispatcherConn.setPassword("ir_dev");
		dispatcherConn.initializeConfiguration();
		connection=dispatcherConn.getConnection();
		System.out.println(connection);
		connection.setAutoCommit(false);
	}
}