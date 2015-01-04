package awsfront;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Scheduler implements Runnable {

	static ServerSocket serv;
	Socket clsock;
	private static int port;
	int choice;
	int num;
	String task;
	ArrayList<Integer> ntask = new ArrayList<Integer>();

	public Scheduler(Socket cSocket) {
		this.clsock = cSocket;
	}

	public static void main(String[] args) throws IOException {

		if (args.length < 2) {
			System.out.println("Wrong invocation");
			System.out.println("java -jar Scheduler.jar –s <PORT>");
			System.exit(0);
		}
		port = Integer.parseInt(args[1]);// Port number for Connection

		System.out.println("Waiting for clients...");
		serv = new ServerSocket(port);
		while (true) {
			Socket clsock = serv.accept();
			// accepting clients connection
			System.out.println("Connection Accepted at Scheduler Side");
			new Thread(new Scheduler(clsock)).start();
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
			task = (String) object;// reading URLs from client
		} catch (ClassNotFoundException e) {
			System.out.println("The task Batch has not come from the Client");
		} catch (IOException e) {
			e.printStackTrace();
		}

		String clientId = task.split(",")[0];

		RemoteScheduler remoteScheduler = new RemoteScheduler(clientId,
				task);
		remoteScheduler.sendTaskToQueue();
		ExecutorService remoteService = Executors.newFixedThreadPool(1);
		try {
			Future<String> remoteResult = remoteService
					.submit(remoteScheduler);
			String clientResult = remoteResult.get();
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

		try {
			clsock.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
