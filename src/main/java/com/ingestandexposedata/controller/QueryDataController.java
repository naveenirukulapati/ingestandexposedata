package com.ingestandexposedata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ingestandexposedata.service.QueryDataService;

@Controller
public class QueryDataController {
	
	@Autowired
	private QueryDataService queryDataService;

	@RequestMapping(value="/getAllListOfS3ObjectsDownloaded", method=RequestMethod.GET)
	public @ResponseBody Object ingestdataFromS3Bucket() {
		return queryDataService.getListAllObjectsDownloaded();
	}
	
	@RequestMapping(value="/download/s3object", method=RequestMethod.GET)
	public ResponseEntity<Object> downloadS3ObjectFile(@RequestParam String s3Objectkey) {
		Resource resource;
		try {
			resource = queryDataService.downloadS3Object(s3Objectkey);
			return ResponseEntity.ok()
	                .contentType(MediaType.parseMediaType("application/octet-stream"))
	                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + s3Objectkey + "\"")
	                .body(resource);
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
            .body("Exception Occured wile processing the request.");
		}
	}
}
