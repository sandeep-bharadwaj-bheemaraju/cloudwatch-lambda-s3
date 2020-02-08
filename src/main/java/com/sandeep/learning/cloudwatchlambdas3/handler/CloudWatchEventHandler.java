package com.sandeep.learning.cloudwatchlambdas3.handler;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class CloudWatchEventHandler implements RequestHandler<ScheduledEvent, String> {

	private AmazonDynamoDB amazonDynamoDB;

	private DynamoDB dynamoDB;

	private AmazonS3 amazonS3;

	private Map<String, String> configMap;

	public CloudWatchEventHandler() {

		amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().build();

		dynamoDB = new DynamoDB(amazonDynamoDB);

		amazonS3 = AmazonS3ClientBuilder.standard().build();

		loadConfiguration();
	}

	@Override
	public String handleRequest(ScheduledEvent event, Context context) {

		context.getLogger().log("Cloud Watch Event is [" +event+ "]");

		cleanUpProcessed(event, context);

		return callLambda(context);
	}


	private void loadConfiguration() {
		configMap = new HashMap<>();

		Table configTable = dynamoDB.getTable("configuration");

		Item item = configTable.getItem("CONFIG_KEY", "BUCKET");
		configMap.put("BUCKET", item.getString("CONFIG_VALUE"));

		item = configTable.getItem("CONFIG_KEY", "LAMBDA");
		configMap.put("LAMBDA", item.getString("CONFIG_VALUE"));

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

				context.getLogger().log("FILE ["+fileName+"] MOVED TO SUCCEEDED STATE");
			}
		} else {

			for(String fileName : files.split(",")) {

				amazonS3.copyObject(configMap.get("BUCKET"), configMap.get("IN-PROCESS-DIR-PATH")+fileName, configMap.get("BUCKET"), configMap.get("FAILED-DIR-PATH")+fileName);
				amazonS3.deleteObject(configMap.get("BUCKET"), configMap.get("IN-PROCESS-DIR-PATH")+fileName);

				context.getLogger().log("FILE ["+fileName+"] MOVED TO FAILED STATE");
			}
		}
	}


	private String callLambda(Context context) {

		AWSLambdaAsync awsLambdaAsync = AWSLambdaAsyncClientBuilder.standard().build();

		LambdaEvent lambdaEvent = new LambdaEvent();
		lambdaEvent.setEventSource("lambda");
		lambdaEvent.setFunctionName(context.getFunctionName());
		lambdaEvent.setFunctionVersion(context.getFunctionVersion());
		lambdaEvent.setArn(context.getInvokedFunctionArn());
		lambdaEvent.setRequestId(context.getAwsRequestId());
		lambdaEvent.setEventTime(DateTime.now().toString());

		InvokeRequest invokeRequest = new InvokeRequest()
										.withFunctionName(configMap.get("LAMBDA"))
										.withPayload(lambdaEvent.toString());

		context.getLogger().log("Invoking ["+configMap.get("LAMBDA")+"] Lambda Function");

		awsLambdaAsync.invoke(invokeRequest);

		return lambdaEvent.toString();
	}

}
