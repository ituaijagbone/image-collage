package awsfront;

public class RemoteImgTask implements Runnable {
	
	private String imgTask;
	private String clientId;
	private String clientCounter;
	
	public RemoteImgTask(String clientId, String imgTask) {
		this.clientId = clientId;
		this.imgTask = imgTask;
	}
	
	@Override
	public void run() {
		DdbRemoteService ddbRemoteService = new DdbRemoteService();
		String tableName = "ImgTasks";
		ddbRemoteService.createTable(tableName);
		
		// check if task already in 
		String tmpId = this.clientId.replace("_", "");
		boolean isFound = ddbRemoteService.load(tmpId, tableName);
		ImageToVideo imgToVideo;
//		System.out.println("state - " + isFound);
		if (!isFound) {
			ddbRemoteService.insert(tmpId, this.imgTask, tableName);
			StringBuilder message = new StringBuilder();
			message.append(clientId).append("," ).append(this.clientCounter).append(",");
			try {
				String[] urls = imgTask.split("@");
				if (urls.length != 0) {
					imgToVideo = new ImageToVideo(tmpId, urls);
					imgToVideo.createImgFolder();
					imgToVideo.downloadImgs();
					message.append(imgToVideo.collageImgs());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			SqsRemoteService sqsRemoteService = SqsRemoteService.getSqsRemoteService();
			String clientUrl = sqsRemoteService.createClientQueue(
					this.clientId);
			sqsRemoteService.sendMessageToClientQueue(clientUrl, message.toString());
		} else {
//			System.out.print("Skipping");
		}
	}
}
