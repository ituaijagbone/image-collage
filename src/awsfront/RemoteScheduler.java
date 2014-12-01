package awsfront;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;

public class RemoteScheduler implements Callable<String[]>{
	private ArrayList<String> sleepTasks;
	private SqsRemoteService sqsRemoteService;
	private String schedulerUrl;
	private String clientId;
	private List<String> clientResult;
	
	public RemoteScheduler(String clientId, ArrayList<String> sleepTasks) {
		this.sleepTasks = sleepTasks;
		this.sqsRemoteService = SqsRemoteService.getSqsRemoteService();
		this.schedulerUrl = sqsRemoteService.getSchedulerUrl();
		this.clientId = clientId;
	}
	
	public void sendTaskToQueue() {
		List<SendMessageBatchRequestEntry> entries;
		
		if (sleepTasks.size() <= 10) {
			entries = new ArrayList<SendMessageBatchRequestEntry>();
			for (int i = 0; i < sleepTasks.size(); i++) {
				entries.add(new SendMessageBatchRequestEntry("id"+i, 
						sleepTasks.get(i)));
			}
			sqsRemoteService.sendBatchMessageToSchedulerQueue(schedulerUrl, entries);
		} else {
			entries = new ArrayList<SendMessageBatchRequestEntry>();
			for (int i = 0; i < sleepTasks.size(); i++) {
				entries.add(new SendMessageBatchRequestEntry("id"+i, 
						sleepTasks.get(i)));
				if ((i+1) % 10 == 0) {
					sqsRemoteService.sendBatchMessageToSchedulerQueue(schedulerUrl, entries);
					entries = new ArrayList<SendMessageBatchRequestEntry>();
				}
			}
			
			if (entries.size() > 0 && entries.size() <= 10) {
				sqsRemoteService.sendBatchMessageToSchedulerQueue(schedulerUrl, entries);
			}
		}
		
	}

	@Override
	public String[] call() {
		this.sqsRemoteService = SqsRemoteService.getSqsRemoteService();
		clientResult = new ArrayList<String>();
		String clientUrl = sqsRemoteService.getClientUrl(this.clientId);
		System.out.println(clientUrl);
		while (clientResult.size() < sleepTasks.size()) {
			if (clientUrl.isEmpty()) {
				continue;
			}
			List<Message> messages = sqsRemoteService.getMessagesFromClientQueue(clientUrl);
			if (messages == null || messages.size() == 0) {
				System.out.println("No messages in the queue");
				try {
					Thread.sleep(20000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
//				System.out.println("getting messages");
				for(Message message: messages) {
					String clientTask = message.getBody();
					StringTokenizer clTokenizer = new StringTokenizer(clientTask, ",");
					String tmpId = null;
					
					tmpId = clTokenizer.nextToken();
					if (tmpId.equalsIgnoreCase(this.clientId)) {
						String countId = clTokenizer.nextToken();
						String desc = clTokenizer.nextToken();
						if (!clientResult.contains(countId + "," + desc)) {
							clientResult.add(countId + "," + desc);
//							System.out.println("client result size: " + clientResult.size());
						}
					}
				}
			}
		}
		
		return getClientResult();
	}
	
	public String[] getClientResult() {
		return this.clientResult.toArray(new String[clientResult.size()]);
	}
}
