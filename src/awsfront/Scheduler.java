package awsfront;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

public class Scheduler implements Runnable {

	static ServerSocket serv;
	Socket clsock;
	private static int port;
	int choice;
	int num;
	String taskqueue[] = new String[10];
	ArrayList<String> task = new ArrayList<String>();
	ArrayList<Integer> ntask = new ArrayList<Integer>();

	public Scheduler(Socket cSocket, int choice, int num) {
		this.clsock = cSocket;
		this.choice = choice;
		this.num = num;
	}

	public static void main(String[] args) throws IOException {

		if (args.length < 2) {
			System.out.println("Wrong invocation");
			System.out.println("scheduler –s <PORT> -lw <NUM> -rw");
			System.exit(0);
		}
		port = Integer.parseInt(args[1]);// Port number for Connection
		int choice = 1;
		// the number of threads it should be argument
		int num = 10;
		try {
			String choiceType = args[2];
			if (choiceType.equals("-lw")) {
				choice = 1;
				num = Integer.parseInt(args[3]);
			} else {
				choice = 2;
			}

		} catch (IndexOutOfBoundsException e) {
			e.printStackTrace();
		}

		System.out.println("Waiting for clients...");
		serv = new ServerSocket(port);
		while (true) {
			Socket clsock = serv.accept();
			// accepting clients connection
			System.out.println("Connection Accepted at Scheduler Side");
			new Thread(new Scheduler(clsock, choice, num)).start();
		}

	}

	@Override
	public void run() {
		// Reading the tasks from the Client
		// Storing it in a task Queue, in-memory
		try {
			ObjectInputStream objectInput = new ObjectInputStream(
					clsock.getInputStream());
			Object object = objectInput.readObject();
			taskqueue = (String[]) object;// reading string array from client
			for (int i = 0; i < taskqueue.length && taskqueue[i] != null; i++) {
				task.add(taskqueue[i]);
				ntask.add(Integer.parseInt(taskqueue[i].split(",")[2]));
			}
		} catch (ClassNotFoundException e) {
			System.out.println("The task Batch has not come from the Client");
		} catch (IOException e) {
			e.printStackTrace();
		}

		String clientId = task.get(0).split(",")[0];

		if (choice == 1) {
			// ######### Local Worker ########
			ExecutorService localService = Executors.newFixedThreadPool(1);
			try {
				Future<String[]> localResult = localService
						.submit(new LocalScheduler(ntask, num));
				String[] clientResult = localResult.get();
				ObjectOutputStream outputStream = new ObjectOutputStream(
						clsock.getOutputStream());
				outputStream.writeObject(clientResult);
				outputStream.close();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			RemoteScheduler remoteScheduler = new RemoteScheduler(clientId,
					task);
			remoteScheduler.sendTaskToQueue();
			ExecutorService remoteService = Executors.newFixedThreadPool(1);
			try {
				Future<String[]> remoteResult = remoteService
						.submit(remoteScheduler);
				String[] clientResult = remoteResult.get();
				ObjectOutputStream outputStream = new ObjectOutputStream(
						clsock.getOutputStream());
				outputStream.writeObject(clientResult);
				outputStream.close();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			clsock.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
