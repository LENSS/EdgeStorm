package com.lenss.mstorm.core;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.lenss.mstorm.communication.internodes.CommunicationClient;
import com.lenss.mstorm.communication.internodes.CommunicationServer;
import com.lenss.mstorm.executor.Executor;
import com.lenss.mstorm.executor.ExecutorManager;
import com.lenss.mstorm.status.StatusReporter;
import com.lenss.mstorm.topology.BTask;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.utils.MyPair;
import com.lenss.mstorm.utils.Serialization;
import com.lenss.mstorm.zookeeper.Assignment;

public class ComputingNode implements Runnable {

	//private static String jarDirectory= System.getProperty("user.home") + File.separator + "EdgeStorm" + File.separator;
	private static String jarDirectory= "/";
	//// EXECUTORS
	private ExecutorManager mExecutorManager;
	private LinkedBlockingDeque<Runnable> mExecutorQueue;
	private static final int NUMBER_OF_CORE_EXECUTORS = 4;
	private static final int NUMBER_OF_MAX_EXECUTORS = 8;
	private static final int KEEP_ALIVE_TIME = 60;
	private static final TimeUnit KEEP_ALIVE_TIME_UNIT = TimeUnit.SECONDS;

	// pause or continue stream processing;, 0 continue, 1 pause
	private static int pauseOrContinue = 0;
	private StatusReporter statusReporter;
	private Thread reporterThread;
	private Dispatcher dispatcher;

	// assignment from scheduler
	private static Assignment assignment;
	private static Topology topology;
	private List<Integer> localTasks;

	//// COMMUNICATION
	// sever and client on computing nodes
	private CommunicationServer mServer;
	private CommunicationClient mClient;

	//// DISTINCT DATA QUEUES FOR TASKS
	// data queues
	public static Map<Integer, BlockingQueue<byte[]>> incomingQueues = new HashMap<Integer, BlockingQueue<byte[]>>();
	public static Map<Integer, BlockingQueue<MyPair<String, byte[]>>> outgoingQueues = new HashMap<Integer, BlockingQueue<MyPair<String, byte[]>>>();
	// result queues
	public static ConcurrentHashMap<Integer, BlockingQueue<MyPair<String, byte[]>>> resultQueues = new ConcurrentHashMap<Integer, BlockingQueue<MyPair<String, byte[]>>>();

	//// DISTINCT STATUS QUEUES FOR TASKS
	// entry times of input
	public static Map<Integer, CopyOnWriteArrayList<Long>> task2EntryTimesForFstComp = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
	// entry times of tuple
	public static Map<Integer, CopyOnWriteArrayList<Long>> task2EntryTimes = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
	// time beginning processing
	public static Map<Integer, CopyOnWriteArrayList<Long>> task2BeginProcessingTimes = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
	// For UpStream use: emit times of tuple
	public static Map<Integer, CopyOnWriteArrayList<Long>> task2EmitTimesUpStream = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
	// For UpStream use: delay of tuple at task
	public static Map<Integer, CopyOnWriteArrayList<Long>> task2ResponseTimesUpStream = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
	// For UpStream use: processing delay of tuple at task
	public static Map<Integer, CopyOnWriteArrayList<Long>> task2ProcessingTimesUpStream = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
	// For Nimbus use: emit times of tuple
	public static Map<Integer, CopyOnWriteArrayList<Long>> task2EmitTimesNimbus = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
	// For Nimbus use: delay of tuple at task
	public static Map<Integer, CopyOnWriteArrayList<Long>> task2ResponseTimesNimbus = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
	// total tuple number from task to downstream tasks in a sampling period
	public static Map<Integer, ConcurrentHashMap<Integer, Integer>> task2taskTupleNum = new HashMap<Integer, ConcurrentHashMap<Integer, Integer>>();
	// total tuple size from task to downstream tasks in a sampling period
	public static Map<Integer, ConcurrentHashMap<Integer, Long>> task2taskTupleSize = new HashMap<Integer, ConcurrentHashMap<Integer, Long>>();

	public ComputingNode(Assignment assign) {
		assignment = assign;
		topology = new Gson().fromJson(assignment.getSerTopology(), Topology.class);
		mExecutorQueue = new LinkedBlockingDeque<Runnable>();
		mExecutorManager = new ExecutorManager(NUMBER_OF_CORE_EXECUTORS, NUMBER_OF_MAX_EXECUTORS, KEEP_ALIVE_TIME,
				KEEP_ALIVE_TIME_UNIT, mExecutorQueue);
	}

	public void run() {
		pauseOrContinue = 0; // allow pulling more tuple for stream processing

		//// create a status reporter
		// statusReporter = StatusReporter.getInstance();
		// statusReporter.initializeStatusReporter(Supervisor.mHandler,
		//// this.getApplicationContext(), Supervisor.masterNodeClient,
		//// Supervisor.cluster_id);

		HashMap<Integer, String> task2Component = assignment.getTask2Component();
		localTasks = getLocalTasks(MStormWorker.localAddress, assignment);
		if (localTasks != null) {
			for (int i = 0; i < localTasks.size(); i++) {
				int taskId = localTasks.get(i);
				ComputingNode.addQueuesForTask(taskId);
				// Add result queue for reporting results to mobile app
				String lastComponent = topology.getComponents()[topology.getComponentNum() - 1];
				if (task2Component.get(taskId).equals(lastComponent)) {
					ComputingNode.addResultQueuesForTask(taskId);
				}
				// Add task2EntryTimesForFstComp to get the input rate of mobile app
				String fstComponent = topology.getComponents()[0];
				if (task2Component.get(taskId).equals(fstComponent)) {
					ComputingNode.addTask2EntryTimesForFstComp(taskId);
				}
			}
		}

		// setup server and client for communication with other nodes
		if (mServer != null) {
			mServer.release();
		}
		mServer = new CommunicationServer();
		mServer.setup();

		System.out.println("The server is up ..." + "ip:" + mServer.addr);
		
		if (mClient != null) {
			mClient.release();
		}
		mClient = new CommunicationClient();
		mClient.setup();

		// wait for setting up of all nodes
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// establish connections
		connectTasks();

		// wait for connections
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// execute tasks assigned to this node

		String jarFileName = assignment.getFileName() + ".jar";
		String jarFilePath = jarDirectory+jarFileName;
		File jarFile = new File(jarFilePath);
		ClassLoader ucLoader = null;
		try {
			URL jarFileURL = jarFile.toURI().toURL();
			URL urls[] = { new URL("jar:" + jarFileURL + "!/") };
			ucLoader = URLClassLoader.newInstance(urls, getClass().getClassLoader());
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		if (ucLoader != null) {
			if (localTasks != null) {
				HashMap<String, String> component2serInstance = topology.getSerInstances();
				for (int i = 0; i < localTasks.size(); i++) {
					int taskID = localTasks.get(i);
					String component = task2Component.get(taskID);
					String sourceIP = assignment.getSourceIP();
					String instance = component2serInstance.get(component);
					try {
						Class<?> mClass = ucLoader.loadClass(component);
						BTask mTask = (BTask) Serialization.Deserialize(instance, mClass);
						mTask.setTaskID(taskID);
						mTask.setComponent(component);
						mTask.setSourceIP(sourceIP);
						Executor taskExecutor = new Executor(mTask);
						mExecutorManager.submitTask(taskID, taskExecutor);
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
			}
		}

		// wait for all tasks running up
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		dispatcher = new Dispatcher();
		Thread dispatchThread = new Thread(dispatcher);
		dispatchThread.start();

		// reporterThread = new Thread(statusReporter);
		// reporterThread.setPriority(Thread.MAX_PRIORITY);
		// reporterThread.start();

	}

	public void connectTasks() {
		HashMap<Integer, MyPair<Integer, Integer>> port2TaskPair = assignment.getPort2TaskPair();
		HashMap<Integer, String> task2Node = assignment.getTask2Node();

		if (localTasks != null) {
			for (int i = 0; i < localTasks.size(); i++) {
				int taskId = localTasks.get(i);
				Iterator<Map.Entry<Integer, MyPair<Integer, Integer>>> it = port2TaskPair.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<Integer, MyPair<Integer, Integer>> entry = it.next();
					if (entry.getValue().left.equals(taskId)) {
						int localPort = entry.getKey();
						int remoteTaskId = entry.getValue().right;
						String remoteAddress = task2Node.get(remoteTaskId);
						mClient.connect(new InetSocketAddress(remoteAddress, CommunicationServer.SERVER_PORT),
								new InetSocketAddress(MStormWorker.localAddress, localPort));
					}
				}
			}
		}
	}

	public static int getPauseOrContinue(){return pauseOrContinue;}

    public static void setPauseOrContinue(int p){pauseOrContinue = p;}
    
	// get assignment
	public static Assignment getAssignment() {
		return assignment;
	}

	// get tasks assigned to this computing node
	public static ArrayList<Integer> getLocalTasks(String localAddress, Assignment assign) {
		HashMap<String, ArrayList<Integer>> node2Tasks = assign.getNode2Tasks();
		return node2Tasks.get(localAddress);
	}

	// add queues for tasks
	public static void addQueuesForTask(Integer taskID) {
		// add queues for data
		BlockingQueue<byte[]> incomingQueue = new LinkedBlockingDeque<byte[]>();
		BlockingQueue<MyPair<String, byte[]>> outgoingQueue = new LinkedBlockingDeque<MyPair<String, byte[]>>();
		incomingQueues.put(taskID, incomingQueue);
		outgoingQueues.put(taskID, outgoingQueue);
		// add queues for status report
		CopyOnWriteArrayList<Long> entryTimes = new CopyOnWriteArrayList<Long>();
		task2EntryTimes.put(taskID, entryTimes);

		CopyOnWriteArrayList<Long> beginProcessingTimes = new CopyOnWriteArrayList<Long>();
		task2BeginProcessingTimes.put(taskID, beginProcessingTimes);

		CopyOnWriteArrayList<Long> emitTimesUpStream = new CopyOnWriteArrayList<Long>();
		task2EmitTimesUpStream.put(taskID, emitTimesUpStream);

		CopyOnWriteArrayList<Long> responseTimesUpStream = new CopyOnWriteArrayList<Long>();
		task2ResponseTimesUpStream.put(taskID, responseTimesUpStream);

		CopyOnWriteArrayList<Long> processingTimesUpStream = new CopyOnWriteArrayList<Long>();
		task2ProcessingTimesUpStream.put(taskID, processingTimesUpStream);

		CopyOnWriteArrayList<Long> emitTimesNimbus = new CopyOnWriteArrayList<Long>();
		task2EmitTimesNimbus.put(taskID, emitTimesNimbus);

		CopyOnWriteArrayList<Long> responseTimesNimbus = new CopyOnWriteArrayList<Long>();
		task2ResponseTimesNimbus.put(taskID, responseTimesNimbus);

		ConcurrentHashMap<Integer, Integer> task2TupleNum = new ConcurrentHashMap<Integer, Integer>();
		ConcurrentHashMap<Integer, Long> task2TupleSize = new ConcurrentHashMap<Integer, Long>();
		HashSet<MyPair<Integer, Integer>> taskPair = new HashSet<MyPair<Integer, Integer>>(
				assignment.getPort2TaskPair().values());
		for (MyPair<Integer, Integer> pair : taskPair) {
			if (pair.left.equals(taskID)) {
				task2TupleNum.put(pair.right, 0);
				task2TupleSize.put(pair.right, new Long(0));
			}
		}
		task2taskTupleNum.put(taskID, task2TupleNum);
		task2taskTupleSize.put(taskID, task2TupleSize);
	}

	// add result queues for last component tasks
	public static void addResultQueuesForTask(Integer taskID) {
		BlockingQueue<MyPair<String, byte[]>> resultQueue = new LinkedBlockingDeque<MyPair<String, byte[]>>();
		resultQueues.put(taskID, resultQueue);
	}

	// add stream input queues for mobile app at the first component tasks
	public static void addTask2EntryTimesForFstComp(Integer taskID) {
		CopyOnWriteArrayList<Long> entryTimes = new CopyOnWriteArrayList<Long>();
		task2EntryTimesForFstComp.put(taskID, entryTimes);
	}

	// Add tuple to the incoming queue to process
	public static void collect(int taskid, byte[] data) throws InterruptedException {
//		try {
//			Thread.sleep(1);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}

		if (incomingQueues.get(taskid) != null) {
			Long entryTime = System.nanoTime();
			task2EntryTimes.get(taskid).add(entryTime);

			HashMap<Integer, String> task2Component = assignment.getTask2Component();
			String fstComponent = topology.getComponents()[0];
			if (task2Component.get(taskid).equals(fstComponent)) {
				task2EntryTimesForFstComp.get(taskid).add(entryTime);
			}

			incomingQueues.get(taskid).put(data);
		}
	}

	// API for user: Retrieve rx tuple from incoming queue to process
	public static byte[] retrieveIncomingQueue(int taskid) {
//
//		try {
//			Thread.sleep(1);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}

		if (incomingQueues.get(taskid) != null) {
			byte[] incomingData = null;
			try {
				incomingData = incomingQueues.get(taskid).take();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (incomingData != null) {
				Long beginProcessingTime = System.nanoTime();
				task2BeginProcessingTimes.get(taskid).add(beginProcessingTime);
			}
			return incomingData;
		} else
			return null;
	}

	// API for user: Add tuple to the outgoing queue for tx
	public static void emit(byte[] data, int taskid, String Component) throws InterruptedException {
		if (outgoingQueues.get(taskid) != null) {
			MyPair<String, byte[]> outData = new MyPair<String, byte[]>(Component, data);
			outgoingQueues.get(taskid).put(outData);
		}
	}

	// Retrieve tuple from outgoing queue to tx
	public static MyPair<String, byte[]> retrieveOutgoingQueue(int taskid) {

//		try {
//			Thread.sleep(1);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		
		if (outgoingQueues.get(taskid) != null) {
			MyPair<String, byte[]> outGoingData = null;
			try {
				outGoingData = outgoingQueues.get(taskid).take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return outGoingData;
		} else
			return null;
	}
	
	// Add tuple back to the outgoing queue to wait for the tx channel
	public static void reQueue(int taskid, MyPair<String, byte[]> pair) throws InterruptedException {
		if (outgoingQueues.get(taskid) != null)
			((LinkedBlockingDeque<MyPair<String, byte[]>>) outgoingQueues.get(taskid)).putFirst(pair);
	}

	// Add processing results to result queue to send to the source
	public static void emitToResultQueue(int taskid, MyPair<String, byte[]> pair) throws InterruptedException {
		if (resultQueues.get(taskid) != null)
			((LinkedBlockingDeque<MyPair<String, byte[]>>) resultQueues.get(taskid)).putFirst(pair);
	}

	// API for user: Retrieve processing results from result queue to sent back to
	// the user app
	public static MyPair<String, byte[]> retrieveResultQueue(int taskid) {
//		try {
//			Thread.sleep(1);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		
		if (resultQueues.get(taskid) != null) {
			MyPair<String, byte[]> result = null;
			try {
				result = resultQueues.get(taskid).take();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			return result;
		} else
			return null;
	}

	// Check if all tuples in MStorm has been processed
	public static boolean noMoreTupleInMStorm() {
		Iterator it = incomingQueues.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			if (((BlockingQueue) entry.getValue()).size() != 0)
				return false;
		}

		it = outgoingQueues.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			if (((BlockingQueue) entry.getValue()).size() != 0)
				return false;
		}

		it = resultQueues.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			if (((BlockingQueue) entry.getValue()).size() != 0)
				return false;
		}
		return true;
	}

	// Clear records of computing serivces
	public static void removeTaskQueues() {
		// clear all queues

		incomingQueues.clear();

		outgoingQueues.clear();

		resultQueues.clear();

		task2EntryTimesForFstComp.clear();

		task2EntryTimes.clear();

		task2BeginProcessingTimes.clear();

		task2EmitTimesUpStream.clear();

		task2ResponseTimesUpStream.clear();

		task2ProcessingTimesUpStream.clear();

		task2EmitTimesNimbus.clear();

		task2ResponseTimesNimbus.clear();

		task2taskTupleNum.clear();

		task2taskTupleSize.clear();

		System.out.println("All queues removed ... ");
	}
}