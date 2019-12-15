package com.lenss.mstorm.executor;

import java.util.HashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ExecutorManager extends ThreadPoolExecutor {

    // record all the tasks running in the thread pool
    public HashMap<Integer, Future<?>> taskID2Future= new HashMap<Integer, Future<?>>();

    public ExecutorManager(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, LinkedBlockingDeque<Runnable> workQueue) {
        super(corePoolSize,maximumPoolSize,keepAliveTime,unit,workQueue);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (t == null && r instanceof Future) {
            try {
                Object result = ((Future) r).get();
            }
            catch (CancellationException ce) {
                t = ce;
            } catch (ExecutionException ee) {
                t = ee.getCause();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt(); // ignore/reset
            }
        }
        if (t != null){
            t.printStackTrace();
        }

    }

    public void submitTask(Integer taskID, Runnable runnableTask){
        Future<?> future = submit(runnableTask);
        taskID2Future.put(taskID,future);
    }

    public void cancelTask(Integer taskID){
        Future<?> future = taskID2Future.get(taskID);
        future.cancel(true);
        taskID2Future.remove(taskID);
    }
}
