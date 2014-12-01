package awsfront;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.util.Tables;

public class DdbRemoteService {
	private AWSCredentials credentials;
	AmazonDynamoDBClient client;
	private DynamoDBMapper mapper;

	public DdbRemoteService() {
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
			this.client = new AmazonDynamoDBClient(this.credentials);
			Region usWest2 = Region.getRegion(Regions.US_WEST_2);
			client.setRegion(usWest2);
			mapper = new DynamoDBMapper(client);

		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location (~/.aws/credentials), and is in valid format.",
					e);
		}
	}

	public void createTable(String tableName) {
		if (Tables.doesTableExist(client, tableName)) {
			return;
		}

		try {
			CreateTableRequest createTableRequest = new CreateTableRequest()
					.withTableName(tableName);
			createTableRequest.withKeySchema(new KeySchemaElement()
					.withAttributeName("Id").withKeyType(KeyType.HASH));
			createTableRequest
					.withAttributeDefinitions(new AttributeDefinition()
							.withAttributeName("Id").withAttributeType(
									ScalarAttributeType.N));
			createTableRequest
					.withProvisionedThroughput(new ProvisionedThroughput()
							.withReadCapacityUnits(10L).withWriteCapacityUnits(
									10L));
			client.createTable(createTableRequest);
			// TableDescription createTableDescription = client.createTable(
			// createTableRequest).getTableDescription();
			// System.out.println("Created Table: " + createTableDescription);

			waitForTableToBecomeActive(tableName);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		} catch (AmazonClientException e) {
			e.printStackTrace();
		}
	}

	private void waitForTableToBecomeActive(String tableName) {
		System.out.println("Waiting for " + tableName + " to become ACTIVE...");

		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (System.currentTimeMillis() < endTime) {
			DescribeTableRequest request = new DescribeTableRequest()
					.withTableName(tableName);
			TableDescription tableDescription = client.describeTable(request)
					.getTable();
			String tableStatus = tableDescription.getTableStatus();
			System.out.println("  - current state: " + tableStatus);
			if (tableStatus.equals(TableStatus.ACTIVE.toString()))
				return;
			try {
				Thread.sleep(1000 * 20);
			} catch (Exception e) {
			}
		}
		throw new RuntimeException("Table " + tableName + " never went active");
	}

	public void insert(String sleepTaskId, String sleepTask, String tableName) {
		try {
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
			item.put("Id", new AttributeValue().withN(sleepTaskId));
			item.put("sleepTask", new AttributeValue().withS(sleepTask));

			PutItemRequest putItemRequest = new PutItemRequest()
			  .withTableName(tableName)
			  .withItem(item);
			client.putItem(putItemRequest);
//			PutItemResult result = client.putItem(putItemRequest);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void delete(SleepTaskItem sleepTaskItem) {
		// Delete the item.
		mapper.delete(sleepTaskItem);
	}

	public boolean load(String id, String tableName) {
		boolean isFound = false;
		try {
			HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("Id", new AttributeValue().withN(id));

			GetItemRequest getItemRequest = new GetItemRequest().withTableName(
					tableName).withKey(key);

			GetItemResult result = client.getItem(getItemRequest);
			if (result.getItem() != null) {
				isFound = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return isFound;
	}
}
