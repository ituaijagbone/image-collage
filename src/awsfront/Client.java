package awsfront;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

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

	public void send(String task) throws IOException {
		if (task.length() == 0) {
			System.out.println("no URL passed");
		}
		else {
			try { // for sending the list of filenames
				ObjectOutputStream outputStream = new ObjectOutputStream(
						socketClient.getOutputStream());
				outputStream.writeObject(task);
				System.out.println("URLs haven been batched to Scheduler");
			} catch (IOException e) {
				System.out.println("error sending URL to the Scheduler");
			}
		}
	}

	public void receive() {
		try {
			ObjectInputStream objectInput = new ObjectInputStream(
					socketClient.getInputStream());
			Object object;
			object = objectInput.readObject();
			String result = (String) object;
			System.out.println("Task has been completed by scheduler");
			System.out.println("Movie storage location: " + result);
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
		// File name (Load file ) as Argument 2
		if (args.length < 4) {
			System.out.println("Wrong invocation");
			System.out.println("java -jar Client.jar –s <IP_ADDRESS:PORT> -w <LIST_FILE>");
			System.exit(0);
		}
		String[] serverAddress = args[1].split(":");
		
		try {
			Integer.parseInt(serverAddress[1]);
		} catch (IndexOutOfBoundsException e) {
			System.out.println("Port number missing");
			System.out.println("client –s <IP_ADDRESS:PORT> -w <LIST_FILE>");
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
		String line = "";
		/**
		 * Concatenate the list of picture urls into one string
		 */
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(clientHost).append(",");
		while ((line = reader.readLine()) != null) {
			if (!line.isEmpty()) {
				strBuilder.append(line).append("@"); // Reading url names line by line
			}
		}
		reader.close();
		fis.close();
//		String mm = strBuilder.toString().split(",")[1];
//		String testStr[] = mm.split("@");
//		for (String string : testStr) {
//			System.out.println(string);
//		}
//		System.exit(1);
		Client client = new Client(serverAddress[0], Integer.parseInt(serverAddress[1]));
		// Connecting with the Scheduler/server
		try {
			client.connect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Host Unknown. Cannot establish Connection");
		}
		
		long startTime = System.nanoTime();
		client.send(strBuilder.toString());
		client.receive();
		double endTime = (double)(System.nanoTime() -startTime)/1000000;
		System.out.println("Total Time taken: " + endTime);
	}

}
