package awsfront;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Scheduler {
	
	static ServerSocket serv;
	private static int port;
	
	public Scheduler(int port)
	{
		this.port=port;
	}
	public static void main(String[] args) throws IOException, InterruptedException {
		
		String taskqueue[]=new String[10];
		int numtask[]=new int[10];
		int pn=9000;//Integer.parseInt(args[0]);//Port number for Connection
		//Scheduler object
		Scheduler sch=new Scheduler(pn);
		
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
			{
					numtask[i]=Integer.parseInt(taskqueue[i]);
					System.out.println(numtask[i]);
			}
		} catch (ClassNotFoundException e) {
			System.out.println("The task Batch has not come from the Client");
			//e.printStackTrace();
		}
		
		//######### Local Worker ########
		
		for(int i=0;i<numtask.length;i++)
		{
			Thread worker=new Thread(new localWorker(numtask[i]));
			worker.start();
		}
	}

}
