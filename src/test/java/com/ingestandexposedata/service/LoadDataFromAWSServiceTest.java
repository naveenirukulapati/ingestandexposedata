package com.ingestandexposedata.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.ingestandexposedata.dto.IngestRequest;
import com.ingestandexposedata.dto.IngestResponse;

@RunWith(SpringRunner.class)
@SpringBootTest
public class LoadDataFromAWSServiceTest {
	
	@Autowired
	private LoadDataFromAWSService loadDataFromAWSService;
	
	@Test
	public void assertFieldsTestWhenSomeFieldsAreNotPopulated() {
		IngestRequest ingestRequest = new IngestRequest();
		ingestRequest.setAwsBucketName("");
		ingestRequest.setAwsObjectKey(null);
		
		boolean value = loadDataFromAWSService.assertRequiredFields(ingestRequest);
		assertTrue(value);
	}
	
	@Test
	public void getFullPathTest() {
		IngestRequest ingestRequest = new IngestRequest();
		ingestRequest.setAwsBucketName("abc");
		ingestRequest.setAwsObjectKey("def");
		
		String fullPath = loadDataFromAWSService.getFullPath(ingestRequest);
		assertNotNull(fullPath);
	}
	
	/*
	 * test loadObjectFromAWSS3 method when s3 bucket is not available.
	 */
	@Test
	public void loadObjectFromAWSS3TestForErrorScenario() {
		IngestRequest ingestRequest = new IngestRequest();
		ingestRequest.setAwsBucketName("abc");
		ingestRequest.setAwsObjectKey("def");
		
		Object returnObject = null;
		try {
			returnObject = loadDataFromAWSService.loadObjectFromAWSS3(ingestRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}
		assertNotNull(returnObject);
	}
	
	/*
	 * test loadObjectFromAWSS3 method when s3 bucket is available.
	 * Status should be success.
	 */
	@Test
	public void loadObjectFromAWSS3Test() {
		IngestRequest ingestRequest = new IngestRequest();
		ingestRequest.setAwsBucketName("nyc-tlc");
		ingestRequest.setAwsObjectKey("misc/taxi _zone_lookup.csv");
		
		IngestResponse returnObject = null;
		try {
			returnObject = (IngestResponse) loadDataFromAWSService.loadObjectFromAWSS3(ingestRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}
		assertEquals("Status is not matching", returnObject.getStatus(), "Success");
	}

}
