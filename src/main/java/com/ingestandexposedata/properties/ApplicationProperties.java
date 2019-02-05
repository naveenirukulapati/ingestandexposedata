package com.ingestandexposedata.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@ConfigurationProperties(prefix = "application.property")
@Configuration("applicationProperties")
public class ApplicationProperties {
	
	private String baseFolder;

	public String getBaseFolder() {
		return baseFolder;
	}
	public void setBaseFolder(String baseFolder) {
		this.baseFolder = baseFolder;
	}

}
