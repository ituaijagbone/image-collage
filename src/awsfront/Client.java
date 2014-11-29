package awsfront;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Client {
	
		private String hostname;
		private int port;
		static Socket socketClient;
		static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		public Client(String hostname, int port){
			this.hostname = hostname;
			this.port = port;
		}
		public void connect() throws IOException{
			socketClient = new Socket(hostname,port);
			System.out.println("Connection Established");
		}
		
		public void send(String [] task)throws IOException
		{ 
			String tsk[] = new String [task.length];
			int i;
			if(task.length == 0)
				System.out.println("no task passed");
			else
			{
				for(i = 0; i < task.length; i++)
				{
					tsk[i] = task[i];
				}
				try
				{ //for sending the list of filenames
					ObjectOutputStream outputStream = new ObjectOutputStream(socketClient.getOutputStream());
					outputStream.writeObject(tsk);
				}
				catch(IOException e)
				{
					System.out.println("error sending TAsk to the Scheduler");
				}
			}
		}
		
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		//Enter the Name/IP of the Server/Scheduler as Argument 1 
		//File name (LOad file ) as Argument 2
		String hostname = args[0];
		String file=args[1];
		
		Client client=new Client(hostname,9002);
		//Connecting with the Scheduler/server
		try {
			client.connect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Host Unknown. Cannot establish Connection");
		}
		//now reading task from the File so that it can be sent as a Batch
		int n=10;
		String task[] = new String[n];
		String path = "C:/Users/SyedSufyan/workspace/awsfront/src/workLoad/" + file;

			FileInputStream fis = new FileInputStream(path);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis)); 
			for(int i = 0; i < n; i++)
			{
				task[i]=reader.readLine();//reading file names line by line
				//System.out.println(task[i]); //task is like "sleep 100"
			}
		client.send(task);
		
	}

}
