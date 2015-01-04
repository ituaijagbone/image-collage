package awsfront;

import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;

import com.amazonaws.services.sqs.model.Message;

public class RemoteScheduler implements Callable<String>{
	private String imgTasks;
	private SqsRemoteService sqsRemoteService;
	private String schedulerUrl;
	private String clientId;
	private String clientResult;
	
	public RemoteScheduler(String clientId, String imgTasks) {
		this.imgTasks = imgTasks;
		this.sqsRemoteService = SqsRemoteService.getSqsRemoteService();
		this.schedulerUrl = sqsRemoteService.getSchedulerUrl();
		this.clientId = clientId;
	}
	
	public void sendTaskToQueue() {
		sqsRemoteService.sendMessageToSchedulerQueue(schedulerUrl, imgTasks);
	}

	@Override
	public String call() {
		this.sqsRemoteService = SqsRemoteService.getSqsRemoteService();
//		clientResult = new ArrayList<String>();
		String clientUrl = sqsRemoteService.getClientUrl(this.clientId);
		System.out.println(clientUrl);
		boolean notFound = true;
		while (notFound) {
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
						clientResult = clTokenizer.nextToken();
						notFound = false;
						break;
					}
				}
			}
		}
		sqsRemoteService.deleteQueue(clientUrl);
		return getClientResult();
	}
	
	public String getClientResult() {
		return this.clientResult;
	}
}
