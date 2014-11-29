package awsfront;

public class localWorker implements Runnable {

	int sleepTime;
	private int n=1;
	
	
	public localWorker(int sleepTime) {
		this.sleepTime=sleepTime;
	}
	public void setN(int n)
	{
		this.n=n;
	}
	public int getN()
	{
		return n;
	}
	@Override
	public void run() {
		double startTime, endTime, duration;
		// TODO Auto-generated method stub
		
		Thread i=Thread.currentThread();
		try {
			startTime=System.nanoTime();
			Thread.sleep(sleepTime);
			endTime=System.nanoTime();
			duration=(endTime-startTime)/1000000;
			System.out.println("Thread "+i+" Completed Sleep Job : " +sleepTime+ " in Time : "+duration+ " MilliSeconds" );
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			setN(0);
		}
		
	}

}
