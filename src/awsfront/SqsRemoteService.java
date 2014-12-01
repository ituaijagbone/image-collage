package awsfront;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.Message;

public class SqsRemoteService {
	AWSCredentials credentials = null;
	AmazonSQS sqs = null;
	private String schedulerQueue = "MyQueue";
	private static volatile SqsRemoteService sqsRemoteService = 
			new SqsRemoteService();
	public SqsRemoteService() {
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
			this.sqs = new AmazonSQSClient(this.credentials);
			Region usWest2 = Region.getRegion(Regions.US_WEST_2);
			sqs.setRegion(usWest2);
			
		} catch (Exception e) {
			 throw new AmazonClientException(
					 "Cannot load the credentials from the credential profiles file. " +
					 "Please make sure that your credentials file is at the correct " +
					 "location (~/.aws/credentials), and is in valid format.",
					 e);
		}
	}
	
	public static SqsRemoteService getSqsRemoteService() {
		return sqsRemoteService;
	}
	
	public AmazonSQS getSqs() {
		return sqsRemoteService.sqs;
	}
	
	public String getSchedulerQueueName() {
		return this.schedulerQueue;
	}
	
	public String getSchedulerUrl() {
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(this.schedulerQueue);
		return sqs.createQueue(createQueueRequest).getQueueUrl();
	}
	
	public String createSchedulerQueue(String schedulerName) {
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(schedulerName);
		return sqs.createQueue(createQueueRequest).getQueueUrl();
	}
	
	public String createClientQueue(String clientName) {
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(clientName);
		return sqs.createQueue(createQueueRequest).getQueueUrl();
	}
	
	public String getClientUrl(String clientName) {
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(clientName);
		return sqs.createQueue(createQueueRequest).getQueueUrl();
	}
	
	public void sendMessageToClientQueue(String queueUrl, String message) {
		this.sqs.sendMessage(new SendMessageRequest(queueUrl, message));
	}
	
	public void sendMessageToSchedulerQueue(String queueUrl, String message) {
		SendMessageResult messageResult = this.sqs.sendMessage(new SendMessageRequest(queueUrl, message));
		System.out.println(messageResult.toString());
	}
	
	public void sendBatchMessageToSchedulerQueue(String queueUrl, 
			List<SendMessageBatchRequestEntry> batchEntries) {
		SendMessageBatchRequest batchRequest = new SendMessageBatchRequest(queueUrl);
		batchRequest.setEntries(batchEntries);
		this.sqs.sendMessageBatch(batchRequest);
	}
	
	public List<Message> getMessagesFromScheduler(String queueUrl) {
		ReceiveMessageRequest messageRequest = new ReceiveMessageRequest(queueUrl);
		List<Message> messages = sqs.receiveMessage(messageRequest).getMessages();
		return messages;
	}
	
	public List<Message> getMessagesFromClientQueue(String queueUrl) {
		ReceiveMessageRequest messageRequest = new ReceiveMessageRequest(queueUrl);
		messageRequest.setWaitTimeSeconds(20);
		List<Message> messages = sqs.receiveMessage(messageRequest).getMessages();
		return messages;
	}

}
