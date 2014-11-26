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
		Thread i=Thread.currentThread();
		try {
			System.out.println("Inside worker thread "+i+" with sleep : " +sleepTime);
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
