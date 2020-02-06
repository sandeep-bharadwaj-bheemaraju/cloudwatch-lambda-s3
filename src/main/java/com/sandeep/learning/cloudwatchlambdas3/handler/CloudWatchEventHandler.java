package com.sandeep.learning.cloudwatchlambdas3.handler;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.glue.model.StartCrawlerRequest;
import com.amazonaws.services.glue.model.StartJobRunRequest;
import com.amazonaws.services.glue.model.StartJobRunResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;

import java.util.HashMap;
import java.util.Map;

public class CloudWatchEventHandler implements RequestHandler<ScheduledEvent, String> {

	private AmazonDynamoDB amazonDynamoDB;

	private DynamoDB dynamoDB;

	private AmazonS3 amazonS3;

	private AWSGlue awsGlue;

	private Map<String, String> configMap;

	public CloudWatchEventHandler() {

		amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().build();

		dynamoDB = new DynamoDB(amazonDynamoDB);

		amazonS3 = AmazonS3ClientBuilder.standard().build();

		awsGlue =  AWSGlueClientBuilder.standard().build();

		loadConfiguration();
	}

	@Override
	public String handleRequest(ScheduledEvent event, Context context) {

		context.getLogger().log("Cloud Watch Event is [" +event+ "]");

		cleanUpProcessed(event, context);

		inspectReady(context);

		return "Cloud Watch Event Triggered Lambda";
	}


	private void loadConfiguration() {
		configMap = new HashMap<>();

		Table configTable = dynamoDB.getTable("configuration");

		Item item = configTable.getItem("CONFIG_KEY", "BUCKET");
		configMap.put("BUCKET", item.getString("CONFIG_VALUE"));

		item = configTable.getItem("CONFIG_KEY", "JOB");
		configMap.put("JOB", item.getString("CONFIG_VALUE"));

		item = configTable.getItem("CONFIG_KEY", "CRAWLER");
		configMap.put("CRAWLER", item.getString("CONFIG_VALUE"));

		item = configTable.getItem("CONFIG_KEY", "READY-DIR-PATH");
		configMap.put("READY-DIR-PATH", item.getString("CONFIG_VALUE"));

		item = configTable.getItem("CONFIG_KEY", "IN-PROCESS-DIR-PATH");
		configMap.put("IN-PROCESS-DIR-PATH", item.getString("CONFIG_VALUE"));

		item = configTable.getItem("CONFIG_KEY", "SUCCEEDED-DIR-PATH");
		configMap.put("SUCCEEDED-DIR-PATH", item.getString("CONFIG_VALUE"));

		item = configTable.getItem("CONFIG_KEY", "FAILED-DIR-PATH");
		configMap.put("FAILED-DIR-PATH", item.getString("CONFIG_VALUE"));
	}


	private void cleanUpProcessed(ScheduledEvent event, Context context) {

		String jobRunId = event.getDetail().get("jobRunId").toString();
		String state = event.getDetail().get("state").toString();

		UpdateItemRequest updateItemRequest = new UpdateItemRequest()
				.withTableName("jobs")
				.addKeyEntry("JOB_ID",new AttributeValue().withS(jobRunId))
				.addAttributeUpdatesEntry("STATE",
						new AttributeValueUpdate().withValue(new AttributeValue().withS(state)));

		AmazonDynamoDBClientBuilder.standard().build().updateItem(updateItemRequest);

		Table jobsTable = dynamoDB.getTable("jobs");

		Item item = jobsTable.getItem("JOB_ID", jobRunId);
		String files = item.getString("FILES");

		if("SUCCEEDED".equals(state)) {

			for(String fileName : files.split(",")) {

				amazonS3.copyObject(configMap.get("BUCKET"), configMap.get("IN-PROCESS-DIR-PATH")+fileName, configMap.get("BUCKET"), configMap.get("SUCCEEDED-DIR-PATH")+fileName);
				amazonS3.deleteObject(configMap.get("BUCKET"), configMap.get("IN-PROCESS-DIR-PATH")+fileName);

				context.getLogger().log("FILE ["+fileName+"] MOVED TO ["+configMap.get("SUCCEEDED-DIR-PATH")+"] STATE");
			}
		} else {

			for(String fileName : files.split(",")) {

				amazonS3.copyObject(configMap.get("BUCKET"), configMap.get("IN-PROCESS-DIR-PATH")+fileName, configMap.get("BUCKET"), configMap.get("FAILED-DIR-PATH")+fileName);
				amazonS3.deleteObject(configMap.get("BUCKET"), configMap.get("IN-PROCESS-DIR-PATH")+fileName);

				context.getLogger().log("FILE ["+fileName+"] MOVED TO ["+configMap.get("FAILED-DIR-PATH")+"] STATE");
			}

		}

	}

	private void inspectReady(Context context) {

		StringBuilder inProcessFiles = new StringBuilder();
		String fileName;

		ObjectListing readyFileList = amazonS3.listObjects(new ListObjectsRequest().withBucketName(configMap.get("BUCKET")).withPrefix(configMap.get("READY-DIR-PATH")));
		int readyFilesCount = readyFileList.getObjectSummaries().size()-1;

		context.getLogger().log("READY STATE FILES ARE - "+readyFilesCount);

		for (S3ObjectSummary summary : readyFileList.getObjectSummaries()) {
			context.getLogger().log("[" +summary.getKey()+ "]");
		}

		ObjectListing inProcessFileList = amazonS3.listObjects(new ListObjectsRequest().withBucketName(configMap.get("BUCKET")).withPrefix(configMap.get("IN-PROCESS-DIR-PATH")));
		int inProcessFilesCount = inProcessFileList.getObjectSummaries().size()-1;

		context.getLogger().log("IN-PROCESS STATE FILES ARE - "+inProcessFilesCount);

		for (S3ObjectSummary summary : inProcessFileList.getObjectSummaries()) {
			context.getLogger().log("[" +summary.getKey()+ "]");
		}

		if(inProcessFilesCount == 0 && readyFilesCount > 0) {

			for (S3ObjectSummary summary : readyFileList.getObjectSummaries()) {

				fileName = summary.getKey().substring(summary.getKey().lastIndexOf("/") + 1);

				if(!fileName.contains(".csv"))
					continue;

				amazonS3.copyObject(configMap.get("BUCKET"), summary.getKey(), configMap.get("BUCKET"), configMap.get("IN-PROCESS-DIR-PATH") + fileName);

				amazonS3.deleteObject(configMap.get("BUCKET"), summary.getKey());

				context.getLogger().log("FILE ["+fileName+"] MOVED TO IN-PROCESS STATE");

				inProcessFiles.append(fileName).append(",");
			}

			inProcessFiles = inProcessFiles.deleteCharAt(inProcessFiles.length()-1);

			awsGlue.startCrawler(new StartCrawlerRequest().withName(configMap.get("CRAWLER")));

			context.getLogger().log("Starting Glue job [" +configMap.get("JOB")+ "] for files [" +inProcessFiles.toString()+ "]");

			StartJobRunResult startJobRunResult = awsGlue.startJobRun(new StartJobRunRequest().withJobName(configMap.get("JOB")));

			context.getLogger().log("Glue Job Id is [" +startJobRunResult.getJobRunId()+ "]");

			Table jobsTable = dynamoDB.getTable("jobs");

			jobsTable.putItem(new Item().with("JOB_ID",startJobRunResult.getJobRunId()).with("FILES",inProcessFiles.toString()).with("STATE", "IN-PROCESS"));

		} if(inProcessFilesCount == 0 && readyFilesCount == 0) {

			context.getLogger().log("NO FILES TO PROCESS..");

		} else {

			context.getLogger().log("JOB RUN IN-PROGRESS..");

		}

	}

}
