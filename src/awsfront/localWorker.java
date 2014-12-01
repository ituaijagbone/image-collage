package awsfront;

import java.util.concurrent.Callable;

public class localWorker implements Callable<String> {

	int sleepTime;
	
	public localWorker(int sleepTime) {
		this.sleepTime=sleepTime;
	}
	
	@Override
	public String call() {
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
			return "Failure on Sleep Task " + sleepTime;
		}
		
		return "Success on Sleep Task " + sleepTime;
	}

}
