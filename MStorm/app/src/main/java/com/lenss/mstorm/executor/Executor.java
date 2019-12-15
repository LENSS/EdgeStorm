package com.lenss.mstorm.executor;

/** Each executor is a single thread that will run a particular thread
 *
 */


import com.lenss.mstorm.status.StatusReporterEKBased;
import com.lenss.mstorm.topology.BTask;
import android.os.Process;

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
		// StatusReporter reporter = StatusReporter.getInstance();
		StatusReporterEKBased reporter = StatusReporterEKBased.getInstance();
		reporter.addTaskForMonitoring(Process.myTid(), task);
		task.prepare();
		task.execute();
		if (Thread.currentThread().isInterrupted()) {
			logger.info( "Task "+task.getTaskID()+" for component "+ task.getComponent() + "is canceled!");
			task.postExecute();
		}
	}

	public void stop(){
		Thread.currentThread().interrupt();
	}
}
