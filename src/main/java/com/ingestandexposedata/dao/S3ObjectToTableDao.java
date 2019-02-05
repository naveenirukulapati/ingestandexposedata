package com.ingestandexposedata.dao;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class S3ObjectToTableDao {

	@Autowired
	private DataSource dataSource;
	
	JdbcTemplate jdbcTemplate;
	
	public JdbcTemplate getJdbcTemplate() {
		if(jdbcTemplate == null) {
			jdbcTemplate = new JdbcTemplate(dataSource);
		}
		return jdbcTemplate;
	}
	
	public int checkIfAlreadyExistInDb(UUID uuid) {
		List<Map<String, Object>> list = getJdbcTemplate().queryForList("Select id from s3objectnametotablenamemapping where uuid = '" + uuid.toString() + "';");
		if(list.size() > 0 && Integer.parseInt(list.get(0).get("id").toString()) > 0) {
			return Integer.parseInt(list.get(0).get("id").toString());
		}
		return 0;
	}
	
	public void createS3ObjectAsTable(UUID uuid, String filePath) {
		getJdbcTemplate().execute("CREATE TABLE s3_table_" + checkIfAlreadyExistInDb(uuid) + " AS SELECT * FROM CSVREAD('" + filePath +"');");
	}
	
	public void dropS3ObjectAsTable(int tableid) {
		getJdbcTemplate().execute("drop TABLE s3_table_" + tableid + ";");
	}
	
}
