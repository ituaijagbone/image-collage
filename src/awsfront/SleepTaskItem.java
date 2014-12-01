package awsfront;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="SleepTasks")
public class SleepTaskItem {
	private String id;
	private String sleepTask;
	
	@DynamoDBHashKey(attributeName="Id")
	public String getId() { return id; }
	public void setId(String id) { this.id = id; }
	
	@DynamoDBAttribute(attributeName="sleepTask")
	public String getSleepTask() { return sleepTask; }
	public void setSleepTask(String sleepTask) { this.sleepTask = sleepTask; }
	
	@Override
	public String toString() {
		return "client Id: " + id + " sent sleep task for " + sleepTask;
	}
}
