package awsfront;

public class localWorker implements Runnable {

	int sleepTime;
	
	public void sleepJob()
	{
	
	}
	public localWorker(int sleepTime) {
		this.sleepTime=sleepTime;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			System.out.println("Inside worker thread");
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
