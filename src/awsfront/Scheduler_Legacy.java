package awsfront;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Scheduler_Legacy {
	
	static ServerSocket serv;
	private static int port;
	
	public Scheduler_Legacy(int port)
	{
		this.port=port;
	}
	static AmazonDynamoDBClient dynamoDB;
	static AmazonSQS sqs;
	static String myQueueUrl;
	public static AmazonSQS init()
	{
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\SyedSufyan\\.aws\\credentials), and is in valid format.",
                    e);
        }
        //CREATING AN SQS
        AmazonSQS sqs = new AmazonSQSClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);
        return sqs;
	}
	
	public static String createSQS(AmazonSQS sqs)
	{
		System.out.println("Creating a new SQS queue called MyQueue.\n");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue");
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

        // List queues
        System.out.println("Listing all queues in your account.\n");
        for (String queueUrl : sqs.listQueues().getQueueUrls()) {
            System.out.println("  QueueUrl: " + queueUrl);
        }
        System.out.println();
        return myQueueUrl;
	}
	//taking off the dynamo part
	/*private static Map<String, AttributeValue> newItem(int i, String task) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
       item.put("id", new AttributeValue(String.valueOf(i)));
        item.put("task", new AttributeValue(task));
        return item;
    }*/

	public static void main(String[] args) throws IOException, InterruptedException {
		
		String taskqueue[]=new String[10];
		ArrayList<String> task = new ArrayList<String>();
		ArrayList<Integer> ntask = new ArrayList<Integer>();
		int numtask[]=new int[10];
		int pn=9002;//Integer.parseInt(args[0]);//Port number for Connection
		//Scheduler object
		Scheduler_Legacy sch=new Scheduler_Legacy(pn);
		int choice = 2;//Integer.parseInt(args[0]);
		System.out.println("Waiting for clients...");
			serv=new ServerSocket(port);
			Socket clsock=serv.accept();//accepting clients connection
			System.out.println("Connection Accepted at Scheduler Side");
		//Reading the tasks from the Client
			//Storing it in a task Queue, in-memory
		try {
			ObjectInputStream objectInput = new ObjectInputStream(clsock.getInputStream());
			Object object = objectInput.readObject();
			taskqueue =(String[])object;//reading string array from client
			for(int i = 0;i<taskqueue.length&&taskqueue[i]!=null;i++)
			{		task.add(taskqueue[i]);
					numtask[i]=Integer.parseInt(taskqueue[i]);
					ntask.add(numtask[i]);
					//System.out.println(numtask[i]);
			}
		} catch (ClassNotFoundException e) {
			System.out.println("The task Batch has not come from the Client");
			//e.printStackTrace();
		}
		//the number of threads it should be argument
		int num=10;
		Thread worker[] = new Thread[num];
		if (choice==1)
		{
		//######### Local Worker ########
		
		for(int i=0;i<ntask.size();i++)
		{
			localWorker lw=new localWorker(ntask.get(i));
//			worker[i]=new Thread(lw);
//			worker[i].start();
	//	if(Thread.interrupted()!= true)
				//System.out.println("Thread "+i+" started Sleep "+numtask[i]+" Job");
//				if(lw.getN()==0)
//				{System.out.println("Error completing the task in worker "+i);
//				}
					
		}
		}
		else
		{
			//############REMOTE WORKER###########
			//Initializing SQS and DYnamoDB
			sqs=init();
	      /*  //CREATING DYNAMODB table
	     AmazonDynamoDBClient dynamoDB = new AmazonDynamoDBClient(credentials);
	    // Region usWest2 = Region.getRegion(Regions.US_WEST_2);
	     dynamoDB.setRegion(usWest2);*/
			//########SQS########
		        try {
		        	 // Create a queue
		            myQueueUrl=createSQS(sqs);
		            // Receive messages
		            /*System.out.println("Receiving messages from MyQueue.\n");
		            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
		            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		            for (Message message : messages) {
		                System.out.println("  Message");
		                System.out.println("    MessageId:     " + message.getMessageId());
		                System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
		                System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
		                System.out.println("    Body:          " + message.getBody());
		                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
		                    System.out.println("  Attribute");
		                    System.out.println("    Name:  " + entry.getKey());
		                    System.out.println("    Value: " + entry.getValue());
		                }
		            }
		            
		            System.out.println();
		            // Delete a message
		            System.out.println("Deleting a message.\n");
		            String messageRecieptHandle = messages.get(0).getReceiptHandle();
		            sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
					
		            // Delete a queue
		            System.out.println("Deleting the test queue.\n");
		            sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));*/
		           /* //DynamoDB TABLE
		            String tableName = "sleep-task-table";

		            // Create table if it does not exist yet
		            if (Tables.doesTableExist(dynamoDB, tableName)) {
		                System.out.println("Table " + tableName + " is already ACTIVE");
		            } else {
		                // Create a table with a primary hash key named 'name', which holds a string
		                CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
		                    .withKeySchema(new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH))
		                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.S))
		                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
		                    TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
		                System.out.println("Created Table: " + createdTableDescription);

		                // Wait for it to become active
		                System.out.println("Waiting for " + tableName + " to become ACTIVE...");
		                Tables.waitForTableToBecomeActive(dynamoDB, tableName);
		            }
		            // Describe our new table
		            DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
		            TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
		            System.out.println("Table Description: " + tableDescription);
		          */
		            //Once the SQS and DynamoDB tables are created we can Put the task in them  
		            String msg;
		            // Send message to SQS
		            //Map<String, AttributeValue> item;
		            System.out.println("Sending messages to MyQueue.\n");
		            for(int i=0;i<task.size();i++)
		            {
		            	msg=task.get(i);
		            	System.out.println(msg);
		            	sqs.sendMessage(new SendMessageRequest(myQueueUrl, msg ));
		            
		         // Adding items to DYnamoDB table
		            	// i is the ID of the task
		            // second argument is the task
		            
		            
		           	//item = newItem(i,task.get(i));// see the Map description
		           	
		            //PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
		            //PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
		            //System.out.println("Result: " + putItemResult);
		        }
		            System.out.println("Putting the Task in SQS Completed");
		        //#### remove the delete queue part when running real time, unless its required ####
		            // Delete a queue
		            System.out.println("Deleting the test queue.\n");
		            sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
		            
		        } catch (AmazonServiceException ase) {
		            System.out.println("Caught an AmazonServiceException, which means your request made it " +
		                    "to Amazon, but was rejected with an error response for some reason.");
		            System.out.println("Error Message:    " + ase.getMessage());
		            System.out.println("HTTP Status Code: " + ase.getStatusCode());
		            System.out.println("AWS Error Code:   " + ase.getErrorCode());
		            System.out.println("Error Type:       " + ase.getErrorType());
		            System.out.println("Request ID:       " + ase.getRequestId());
		        } catch (AmazonClientException ace) {
		            System.out.println("Caught an AmazonClientException, which means the client encountered " +
		                    "a serious internal problem while trying to communicate with SQS, such as not " +
		                    "being able to access the network.");
		            System.out.println("Error Message: " + ace.getMessage());
		        }
		            
		        
		}
	}
}
