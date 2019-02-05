package com.ingestandexposedata;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

@Service
public class AWSService {
	
	@Autowired
	ResourceLoader resourceLoader;
	
	final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

	//@PostConstruct
	public void downloadS3Object() throws IOException {
		
		try {
		    S3Object o = s3.getObject("nyc-tlc", "misc/taxi _zone_lookup.csv");
		    S3ObjectInputStream s3is = o.getObjectContent();
		    FileOutputStream fos = new FileOutputStream(new File("taxi _zone_lookup.csv"));
		    byte[] read_buf = new byte[1024];
		    int read_len = 0;
		    while ((read_len = s3is.read(read_buf)) > 0) {
		        fos.write(read_buf, 0, read_len);
		    }
		    s3is.close();
		    fos.close();
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		} catch (FileNotFoundException e) {
		    System.err.println(e.getMessage());
		} catch (IOException e) {
		    System.err.println(e.getMessage());
		}
		
//	    Resource resource = resourceLoader.getResource("s3://nyc-tlc/misc/taxi _zone_lookup.csv");
//	    File downloadedS3Object = new File(resource.getFilename());
//	  
//	    try (InputStream inputStream = resource.getInputStream()) {
//	        Files.copy(inputStream, downloadedS3Object.toPath(), 
//	          StandardCopyOption.REPLACE_EXISTING);
//	    }
	}

}
