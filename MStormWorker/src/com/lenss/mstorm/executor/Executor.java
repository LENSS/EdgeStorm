package com.lenss.mstorm.executor;

/** Each executor is a single thread that will run a particular thread
 *
 */

import com.lenss.mstorm.status.StatusReporter;
import com.lenss.mstorm.topology.BTask;
import org.apache.log4j.Logger;

public class Executor implements Runnable {
	private final String TAG="Executor";
	Logger logger = Logger.getLogger(TAG);
	private BTask task;

	public Executor(BTask t) {
        task = t;
	}

	@Override
	public void run() {
		task.prepare();
		task.execute();
		if (Thread.currentThread().isInterrupted()) {
			logger.info( "Task "+task.getTaskID()+" for component "+ task.getComponent() + "has been canceled!");
			task.postExecute();
		}
	}

	public void stop(){
		Thread.currentThread().interrupt();
	}
}

