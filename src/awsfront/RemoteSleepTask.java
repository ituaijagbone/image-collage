package awsfront;

public class RemoteSleepTask implements Runnable {
	
	private String sleepTask;
	private String clientId;
	private String clientCounter;
	
	public RemoteSleepTask(String clientId, String clientCounter, String sleepTask) {
		this.clientId = clientId;
		this.sleepTask = sleepTask;
		this.clientCounter = clientCounter;
	}
	
	@Override
	public void run() {
		DdbRemoteService ddbRemoteService = new DdbRemoteService();
		String tableName = "SleepTasks";
		ddbRemoteService.createTable(tableName);
		
		// check if task already in 
		String tmpId = this.clientId.replace("_", "") + this.clientCounter;
		boolean isFound = ddbRemoteService.load(tmpId, tableName);
//		System.out.println("state - " + isFound);
		if (!isFound) {
			ddbRemoteService.insert(tmpId, this.sleepTask, tableName);
			try {
				System.out.println("Found new tasks");
				System.out.println("Going to sleep for " + this.sleepTask + " millisecond");
				Thread.sleep(Long.parseLong(this.sleepTask));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			SqsRemoteService sqsRemoteService = SqsRemoteService.getSqsRemoteService();
			String clientUrl = sqsRemoteService.createClientQueue(
					this.clientId);
			String message = this.clientId + "," + this.clientCounter + "," + "Task completed";
			sqsRemoteService.sendMessageToClientQueue(clientUrl, message);
		} else {
//			System.out.print("Skipping");
		}
	}
}
