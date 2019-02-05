package com.ingestandexposedata.dto;

public class IngestRequest {
	
	private String awsBucketName;
	private String awsObjectKey;
	private boolean reload;
	
	public String getAwsBucketName() {
		return awsBucketName;
	}
	public void setAwsBucketName(String awsBucketName) {
		this.awsBucketName = awsBucketName;
	}
	public String getAwsObjectKey() {
		return awsObjectKey;
	}
	public void setAwsObjectKey(String awsObjectKey) {
		this.awsObjectKey = awsObjectKey;
	}
	public boolean isReload() {
		return reload;
	}
	public void setReload(boolean reload) {
		this.reload = reload;
	}

}
