package awsfront;

import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.Message;

public class RemoteWorker implements Runnable {
	String schedulerUrl;
	long idleness = 0; 
	public RemoteWorker(String schedulerUrl, long idleness) {
		this.schedulerUrl = schedulerUrl;
		this.idleness = idleness;
	}
	
	public static void main(String[] args) {
		long idleness = 0;
		if (args.length > 0) {
			if (args[0].equalsIgnoreCase("-i")) {
				idleness = Long.parseLong(args[1]) * 1000;
			}
		} else {
			System.out.println("Wrong invocation");
			System.out.println(" java -jar Worker.jar –i num");
			System.exit(0);
		}
		SqsRemoteService sqsRemoteService = SqsRemoteService.getSqsRemoteService();
		
		String schedulerUrl = sqsRemoteService.getSchedulerUrl();
		
		Thread workerThread = new Thread(new RemoteWorker(schedulerUrl, idleness), "Worker - ");
		workerThread.start();
		System.out.println("Remote Worker Started");
		boolean threadState = true;
		while (threadState) {
			if (workerThread.isAlive()) {
				try {
					Thread.sleep(idleness);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				// terminate vm
			}
		}
	}
	
	@Override
	public void run() {
		SqsRemoteService sqsRemoteService = SqsRemoteService.getSqsRemoteService();
		// Create thread pool here, 
		ExecutorService threadService = null;
		for (; ; ) {
			List<Message> messages = sqsRemoteService.getMessagesFromScheduler(this.schedulerUrl);
			if (messages == null || messages.size() == 0) {
				try {
					if ((idleness - System.currentTimeMillis()) < 0 || idleness != 0) {
						// Check if thread pool is not null then stop all jobs
						if (threadService != null) {
							threadService.shutdown();
							try {
								if (!threadService.awaitTermination(60, TimeUnit.SECONDS)) {
									threadService.shutdownNow();
									if (!threadService.awaitTermination(60, TimeUnit.SECONDS)) {
										System.err.println("Thread Service Not Terminating");
									}
								}
							} catch (Exception e) {
								threadService.shutdown();
								Thread.currentThread().interrupt();
							}
						}
					} else {
						Thread.sleep(30000);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				threadService = Executors.newFixedThreadPool(messages.size());
				for(Message message: messages) {
					String clientTask = message.getBody();
					StringTokenizer clTokenizer = new StringTokenizer(clientTask, ",");
					String clientId = null;
					String desc = null;
					
					clientId = clTokenizer.nextToken();
					desc = clTokenizer.nextToken();
					threadService.submit(new RemoteImgTask(clientId, desc));
				}
				
				try {
					Thread.sleep(20000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
