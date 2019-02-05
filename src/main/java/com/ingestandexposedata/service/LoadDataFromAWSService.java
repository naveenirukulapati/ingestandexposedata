package com.ingestandexposedata.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.ingestandexposedata.dao.MappingTableDao;
import com.ingestandexposedata.dao.S3ObjectToTableDao;
import com.ingestandexposedata.dto.IngestRequest;
import com.ingestandexposedata.dto.IngestResponse;
import com.ingestandexposedata.exception.IngestException;
import com.ingestandexposedata.properties.ApplicationProperties;

@Service
public class LoadDataFromAWSService {
	
	final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
	
	@Autowired
	ApplicationProperties applicationProperties;
	
	@Autowired
	private MappingTableDao mappingTableDao;
	
	@Autowired
	private S3ObjectToTableDao s3ObjectToTableDao;
	
	/*
	 * This method will load the S3 object to local system.
	 * Parameters: takes incoming request with S3 Bucket and Object key
	 */
	public Object loadObjectFromAWSS3(IngestRequest ingestRequest) throws IOException {
		Object returnObject;
		if(assertRequiredFields(ingestRequest)) {
			returnObject = new IngestException(4002, "AssertionError", "All the required fields are not provided.");
		}
		S3ObjectInputStream s3ObjectInputStream = null;
		try {
			S3Object s3Object = s3.getObject(ingestRequest.getAwsBucketName(), ingestRequest.getAwsObjectKey());
		    s3ObjectInputStream = s3Object.getObjectContent();
		    returnObject = writeFile(s3ObjectInputStream, ingestRequest);
		} catch (AmazonS3Exception e) {
			returnObject = new IngestException(4003, "AmazonServiceException", "AmazonServiceException occured. Try again after some time.");
		}  finally {
			if(s3ObjectInputStream != null) {
				s3ObjectInputStream.close();
			}
		}
		
		return returnObject;
	}
	
	public boolean assertRequiredFields(IngestRequest ingestRequest) {
		//Check for all required fields are available or not
		if(ingestRequest.getAwsBucketName() == null || ingestRequest.getAwsBucketName().isEmpty()) return true;
		if(ingestRequest.getAwsObjectKey() == null || ingestRequest.getAwsObjectKey().isEmpty()) return true;
		return false;
	}
	
	/*
	 * Gets the full path of the location, which will be used to save the S3 Object,
	 * based on properties file.
	 */
	public String getFullPath(IngestRequest ingestRequest) {
		if(applicationProperties.getBaseFolder() != null && !applicationProperties.getBaseFolder().equals("")) {
			return applicationProperties.getBaseFolder() + File.separatorChar + ingestRequest.getAwsBucketName() + File.separatorChar + ingestRequest.getAwsObjectKey();
		}
		return ingestRequest.getAwsBucketName() + File.separatorChar + ingestRequest.getAwsObjectKey();
	}
	
	/*
	 * This method will create the folders based on S3 object key in the local machine.
	 */
	public void createFolderIfNotExists(File file) {
		File directory = new File(file.getParent());
		if(!directory.exists()) {
			directory.mkdirs();
		}
	}
	
	/*
	 * If S3 object is already downloaded and reingest flag is enabled then it will delte the existing file.
	 */
	public void deleteObjectIfExists(File file) {
		if(file.exists()) {
			file.delete();
		}
	}
	
	/*
	 * Checks if the file already exists or not
	 */
	public boolean checkIfFileEsists(File file) {
		if(file.exists()) {
			return true;
		}
		return false;
	}
	
	/*
	 * This method will write S3 object to the local system and loads in to the H2 inmemory DB.
	 */
	public Object writeFile(S3ObjectInputStream s3ObjectInputStream, IngestRequest ingestRequest) throws IOException {
		Object returnObject;
		try {
			String filePath = getFullPath(ingestRequest);
			downloadS3Object(s3ObjectInputStream, ingestRequest, filePath);
			
			if(filePath.endsWith(".csv")) {
			    UUID uuid = UUID.nameUUIDFromBytes(filePath.getBytes("UTF-8"));
			    ingestS3ObjectintoTable(ingestRequest, uuid, filePath);
			    returnObject = new IngestResponse("Success", "File ingested and loaded to DB successfully.");
			} else {
				returnObject = new IngestResponse("Success", "Since the provided S3 Object is not a CSv file, We have downloaded and saved but not ingested in to DB.");
			}
		} catch (AmazonServiceException e) {
			returnObject = new IngestException(4003, "AmazonServiceException", "AmazonServiceException occured. Try again after some time.");
		} catch (FileNotFoundException e) {
			returnObject = new IngestException(4003, "FileNotFoundException", "FileNotFoundException occured.");
		} catch (IOException e) {
			returnObject = new IngestException(4003, "IOException", "IOException occured.");
		}
		return returnObject;
	}
	
	/*
	 * This method will download the S3 object.
	 */
	public void downloadS3Object(S3ObjectInputStream s3ObjectInputStream, IngestRequest ingestRequest, String filePath) throws IOException {
		FileOutputStream fileOutputStream = null;
		File file = new File(filePath);
		createFolderIfNotExists(file);
		if(ingestRequest.isReload()) {
			deleteObjectIfExists(file);
		} else if(checkIfFileEsists(file)) {
			return;
		}
		fileOutputStream = new FileOutputStream(file);
	    byte[] read_buf = new byte[1024];
	    int read_len = 0;
	    while ((read_len = s3ObjectInputStream.read(read_buf)) > 0) {
	    	fileOutputStream.write(read_buf, 0, read_len);
	    }
	    
	    if(fileOutputStream != null) {
			fileOutputStream.close();
		}
	}
	
	/*
	 * Loads S3 object in to H2 inmemory DB.
	 */
	public void ingestS3ObjectintoTable(IngestRequest ingestRequest, UUID uuid, String filePath) {
		if(mappingTableDao.checkIfMappingtableExist()) {
	    	int tableid = s3ObjectToTableDao.checkIfAlreadyExistInDb(uuid);
	    	if(tableid > 0 && ingestRequest.isReload()) {
	    		s3ObjectToTableDao.dropS3ObjectAsTable(tableid);
	    		mappingTableDao.deleteRecordFromMappingTable(uuid);
	    		mappingTableDao.insertRecordIntoMappingTable(uuid, filePath);
	    		s3ObjectToTableDao.createS3ObjectAsTable(uuid, filePath);
	    	} else if(tableid == 0) {
	    		mappingTableDao.insertRecordIntoMappingTable(uuid, filePath);
	    		s3ObjectToTableDao.createS3ObjectAsTable(uuid, filePath);
	    	}
	    }
	}

}
