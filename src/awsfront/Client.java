package awsfront;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import javax.sound.sampled.Port;

public class Client {

	private String hostname;
	private int port;
	static Socket socketClient;
	static BufferedReader br = new BufferedReader(new InputStreamReader(
			System.in));

	public Client(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}

	public void connect() throws IOException {
		socketClient = new Socket(hostname, port);
		System.out.println("Connection Established");
	}

	public void send(String[] task) throws IOException {
		String tsk[] = new String[task.length];
		int i;
		if (task.length == 0)
			System.out.println("no task passed");
		else {
			for (i = 0; i < task.length; i++) {
				tsk[i] = task[i];
			}
			try { // for sending the list of filenames
				ObjectOutputStream outputStream = new ObjectOutputStream(
						socketClient.getOutputStream());
				outputStream.writeObject(tsk);
				System.out.println("Tasks haven been batched to Scheduler");
			} catch (IOException e) {
				System.out.println("error sending TAsk to the Scheduler");
			}
		}
	}

	public void receive() {
		try {
			ObjectInputStream objectInput = new ObjectInputStream(
					socketClient.getInputStream());
			Object object;
			object = objectInput.readObject();
			String[] result = (String[]) object;
			System.out.println("Task has been completed by scheduler");
			for (int i = 0; i < result.length; i++) {
				System.out.println(result[i]);
			}
			socketClient.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		// Enter the Name/IP of the Server/Scheduler as Argument 1
		// File name (LOad file ) as Argument 2
		if (args.length < 4) {
			System.out.println("Wrong invocation");
			System.out.println("client –s <IP_ADDRESS:PORT> -w <WORKLOAD_FILE>");
			System.exit(0);
		}
		String[] serverAddress = args[1].split(":");
		
		try {
			Integer.parseInt(serverAddress[1]);
		} catch (IndexOutOfBoundsException e) {
			System.out.println("Port number missing");
			System.out.println("client –s <IP_ADDRESS:PORT> -w <WORKLOAD_FILE>");
			System.exit(0);
		}
		File file = new File(args[3]);
		if (!file.isFile() || !file.canRead()) {
			System.out.println("wrong file given");
			System.exit(0);
		}
		FileInputStream fis = new FileInputStream(file);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

		InetAddress address = InetAddress.getLocalHost();
		String clientHost = address.getHostAddress().replace(".", "_");
		ArrayList<String>task = new ArrayList<String>();
		String line = "";
		int taskCounter = 1;
		while ((line = reader.readLine()) != null) {
			task.add(clientHost + "," + taskCounter + "," + line);// reading file names line by line
			taskCounter+=1;
			// System.out.println(task[i]); //task is like "sleep 100"
		}
		reader.close();
		fis.close();
		
		Client client = new Client(serverAddress[0], Integer.parseInt(serverAddress[1]));
		// Connecting with the Scheduler/server
		try {
			client.connect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Host Unknown. Cannot establish Connection");
		}

		// String path = "C:/Users/SyedSufyan/workspace/awsfront/src/workLoad/"
		// + file;
		
		long startTime = System.nanoTime();
		client.send(task.toArray(new String[task.size()]));
		client.receive();
		double endTime = (double)(System.nanoTime() -startTime)/1000000;
		System.out.println("Total Time taken: " + endTime);
		System.out.println("Throughput: " + task.size()/endTime);
	}

}
