# ingestandexposedata
ingestandexposedata sample project

How to Run:
This is a Spring Boot application.
Step1: get code from git bucket
Step2: go to the base folder where pom.xml file is available and compile the code with the followng command "mvn clean install"
Step3: run application using command provided below
java -jar .\target\IngestAndExposeData-0.0.1-SNAPSHOT.jar

Or
There is a Docker file provided in the code
application can be execute using docker image.

Apis Exposed:
1. URI and Body to ingest an object
http://localhost:8888/ingestandexposedata/ingest
  {
	"awsBucketName": "nyc-tlc",
	"awsObjectKey": "misc/taxi _zone_lookup.csv",
	"reload": 1
}

2. 1. URL to list all ingested objects
http://localhost:8888/ingestandexposedata/getAllListOfS3ObjectsDownloaded

3. 1. URL to fetch an ingested object
http://localhost:8888/ingestandexposedata/download/s3object?s3Objectkey=s3object/taxi%20_zone_lookup.csv

Unit test cases:
I have added very few test cases. in reality there should be more test cases covered.
