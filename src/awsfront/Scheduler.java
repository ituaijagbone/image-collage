package awsfront;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Scheduler {
	
	static ServerSocket serv;
	private static int port;
	
	public Scheduler(int port)
	{
		this.port=port;
	}
	public static void main(String[] args) throws IOException, InterruptedException {
		
		String taskqueue[]=new String[10];
		ArrayList<String> task = new ArrayList<String>();
		int numtask[]=new int[10];
		int pn=8000;//Integer.parseInt(args[0]);//Port number for Connection
		//Scheduler object
		Scheduler sch=new Scheduler(pn);
		int choice = 1;//Integer.parseInt(args[0]);
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
					//System.out.println(numtask[i]);
			}
		} catch (ClassNotFoundException e) {
			System.out.println("The task Batch has not come from the Client");
			//e.printStackTrace();
		}
		//the number of threads it should be argument
		int num=10;
		Thread worker[] = new Thread[num];
		if (choice==2)
		{
		//######### Local Worker ########
		
		for(int i=0;i<numtask.length;i++)
		{
			worker[i]=new Thread(new localWorker(numtask[i]));
			worker[i].start();
	//	if(Thread.interrupted()!= true)
				System.out.println("Thread "+i+" started Sleep "+numtask[i]+" Job");
		}
		}
		else
		{
			//############REMOTE WORKER###########
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

		        AmazonSQS sqs = new AmazonSQSClient(credentials);
		        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		        sqs.setRegion(usWest2);
		        try {
		            // Create a queue
		            System.out.println("Creating a new SQS queue called MyQueue.\n");
		            CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue");
		            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

		            // List queues
		            System.out.println("Listing all queues in your account.\n");
		            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
		                System.out.println("  QueueUrl: " + queueUrl);
		            }
		            System.out.println();
		            String msg;
		            // Send a message
		            System.out.println("Sending a messages to MyQueue.\n");
		            for(int i=0;i<task.size();i++)
		            {
		            	msg=task.get(i);
		            	System.out.println(msg);
		            	sqs.sendMessage(new SendMessageRequest(myQueueUrl, msg ));
		            }
		            // Receive messages
		            System.out.println("Receiving messages from MyQueue.\n");
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
		            sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
		        } catch (AmazonServiceException ase) {
		            System.out.println("Caught an AmazonServiceException, which means your request made it " +
		                    "to Amazon SQS, but was rejected with an error response for some reason.");
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
