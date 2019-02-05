package com.ingestandexposedata.dao;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MappingTableDao {
	
	@Autowired
	private DataSource dataSource;
	
	JdbcTemplate jdbcTemplate;
	
	public JdbcTemplate getJdbcTemplate() {
		if(jdbcTemplate == null) {
			jdbcTemplate = new JdbcTemplate(dataSource);
		}
		return jdbcTemplate;
	}
	
	public boolean checkIfMappingtableExist() {
		try {
			getJdbcTemplate().queryForList("Select count(*) from s3objectnametotablenamemapping");
			return true;
		} catch(BadSqlGrammarException e) {
			if(e.getCause().toString().contains("Table \"S3OBJECTNAMETOTABLENAMEMAPPING\" not found;")) {
				getJdbcTemplate().execute("CREATE TABLE S3OBJECTNAMETOTABLENAMEMAPPING(id int auto_increment, uuid varchar, s3objectname varchar);");
				return true;
			}
			return false;
		}
	}
	
	public void deleteRecordFromMappingTable(UUID uuid) {
		getJdbcTemplate().execute("delete from s3objectnametotablenamemapping where uuid = '" + uuid.toString() + "';");
	}
	
	public void insertRecordIntoMappingTable(UUID uuid, String filePath) {
		getJdbcTemplate().execute("INSERT INTO s3objectnametotablenamemapping(uuid, s3objectname) values('" + uuid.toString() + "', '" + filePath +"');");
	}
	
	public List<Map<String, Object>> getAllS3ObjectKeysFromMappingTable() {
		return getJdbcTemplate().queryForList("SELECT s3objectname from s3objectnametotablenamemapping;");
	}

}
