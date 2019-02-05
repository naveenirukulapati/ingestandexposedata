package com.ingestandexposedata.service;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;

import com.ingestandexposedata.dao.MappingTableDao;

@Service
public class QueryDataService {

	@Autowired
	private MappingTableDao mappingTableDao;
	
	/*
	 * Service method to list all the S3 Objects downloaded.
	 */
	public List<String> getListAllObjectsDownloaded() {
		List<String> s3ObjectKeys = new ArrayList<String>();
		try {
			for(Map<String, Object> s3ObjectKey : mappingTableDao.getAllS3ObjectKeysFromMappingTable()) {
				s3ObjectKeys.add(s3ObjectKey.get("s3objectname").toString());
			}
		} catch(Exception e) {
			//Exception occured while reading objects from DB.
		}
		return s3ObjectKeys;
	}
	
	/*
	 * Service method to return Resource stream for a given S3 Object.
	 */
	public Resource downloadS3Object(String s3Objectkey) throws Exception {
		//Check if it exists
		
		Path path = Paths.get(s3Objectkey);
		Resource resource = new UrlResource(path.toUri());
		if(resource.exists()) {
			return resource;
		} else {
			throw new Exception();
		}
		
	}
}
