package awsfront;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LocalScheduler implements Callable<String[]>{
	
	ArrayList<Integer> ntask = new ArrayList<Integer>();
	ArrayDeque<String> localQueue = new ArrayDeque<String>();
	int threadCount;
	
	public LocalScheduler(ArrayList<Integer> ntask, int threadCount) {
		this.ntask = ntask;
		this.threadCount = threadCount;
	}
	
	public synchronized void addResultToQueue(String result) {
		localQueue.add(result);
	}
	
	public String[] getResultFromQueue() {
		return localQueue.toArray(new String[localQueue.size()]);
	}

	@Override
	public String[] call() {
		ExecutorService threadService = Executors.newFixedThreadPool(
				threadCount);
		List<Future<String>> futures = new ArrayList<Future<String>>();
		for (int i = 0; i < ntask.size(); i++) {
			Future<String>future = threadService.submit(
					new localWorker(ntask.get(i)));
			futures.add(future);
		}
		
		for (Future<String> fut : futures) {
			try {
				addResultToQueue(fut.get());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		
		return getResultFromQueue();
	}
	
}
