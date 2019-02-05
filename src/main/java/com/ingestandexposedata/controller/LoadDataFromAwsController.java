package com.ingestandexposedata.controller;

import java.io.IOException;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ingestandexposedata.dto.IngestRequest;
import com.ingestandexposedata.exception.IngestException;
import com.ingestandexposedata.service.LoadDataFromAWSService;

@Controller
public class LoadDataFromAwsController {
	
	@Autowired
	LoadDataFromAWSService loadDataFromAWSService;

	@RequestMapping(value="/ingest", method=RequestMethod.POST)
	public @ResponseBody Object ingestdataFromS3Bucket(@RequestBody @Valid IngestRequest ingestRequest) {
		try {
			return loadDataFromAWSService.loadObjectFromAWSS3(ingestRequest);
		} catch(IOException e) {
			return new IngestException(4001, "IOException", "IOException occured while reading data from S3 bucket.");
		}
	}
}
