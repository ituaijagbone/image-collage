package awsfront;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="ImgTasks")
public class ImgTaskItem {
	private String id;
	private String imgTask;
	
	@DynamoDBHashKey(attributeName="Id")
	public String getId() { return id; }
	public void setId(String id) { this.id = id; }
	
	@DynamoDBAttribute(attributeName="imgTask")
	public String getImgTask() { return imgTask; }
	public void setImgTask(String imgTask) { this.imgTask = imgTask; }
	
	@Override
	public String toString() {
		return "client Id: " + id + " sent image task for " + imgTask;
	}
}
