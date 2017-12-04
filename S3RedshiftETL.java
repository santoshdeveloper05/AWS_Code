package com.amazonaws.lambda.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3RedshiftETL implements RequestHandler<S3Event, String> {

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
	private static final String AWS_ACCESS_KEY_ID ="<key_id>";
	private static final String AWS_SECRET_KEY = "<secret_key>";

	public S3RedshiftETL() {
	}

	S3RedshiftETL(AmazonS3 s3) {
		this.s3 = s3;
	}

	@Override
	public String handleRequest(S3Event event, Context context) {
		context.getLogger().log("Received event: " + event);

		// Get the S3 bucket name from S3 event
		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		System.out.println(bucket);
		//Get the key which defines the file path
		String key = event.getRecords().get(0).getS3().getObject().getKey();
		System.out.println(key);
		
		try {
			
			Connection conn = null;
			Statement stmt = null;
			try {
				// Dynamically load driver at runtime.
				// Redshift JDBC 4.2 driver: com.amazon.redshift.jdbc42.Driver
				// Open a connection and define properties.
				System.out.println("Connecting to database..." + "com.amazon.redshift.jdbc42.Driver");
				Class.forName("com.amazon.redshift.jdbc42.Driver");
				
				//Create a property object and set redshift credentials
				Properties props = new Properties();
				props.setProperty("user", "test");
                props.setProperty("password", "test");
				
				//initialize redshift connection
                conn = DriverManager.getConnection("jdbc:redshift://test-instance.us-east-1.redshift.amazonaws.com:5439/demo", props);
                System.out.println("Redshift Connection established...");
                stmt = conn.createStatement();
                
				//Create table schema
                String movieDDL = "CREATE TABLE IF NOT EXISTS movies (movieId INTEGER NOT NULL, title VARCHAR(200) NOT NULL, genres VARCHAR(500) NOT NULL);";
                System.out.println("movieDDL==="+movieDDL);
                stmt.addBatch(movieDDL);
                
                System.out.println("movieDDL batch added");

				//Copy Movies data from S3 to Redshift Movies table
                String movieDML = "copy movies from 's3://"+ bucket + "/" + key + "' "
                        + "credentials 'aws_access_key_id=" + AWS_ACCESS_KEY_ID
                        + ";aws_secret_access_key=" + AWS_SECRET_KEY + "' csv maxerror 2;";
                System.out.println("movieDML==="+movieDML);
                stmt.addBatch(movieDML);
                System.out.println("movieDML batch added");
                stmt.executeBatch();
                System.out.println("Successfully loaded movies data");
				System.out.println("Done!!");
				stmt.close();
				conn.close();
			} catch (Exception ex) {
				// For convenience, handle all errors here.
				ex.printStackTrace();
			} finally {
				// Finally block to close resources.
				try {
					if (stmt != null)
						stmt.close();
					if (conn != null)
						conn.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
			System.out.println("Finished Redshift ETLs execution..");
			return "execution successful";
		} catch (Exception e) {
			e.printStackTrace();
			context.getLogger().log(String.format("Error getting object %s from bucket %s. Make sure they exist and"
					+ " your bucket is in the same region as this function.", key, bucket));
			throw e;
		}
	}
}
