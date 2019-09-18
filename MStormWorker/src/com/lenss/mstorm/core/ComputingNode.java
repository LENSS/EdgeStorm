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

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.google.gson.Gson;
import com.lenss.mstorm.communication.internodes.ChannelManager;
import com.lenss.mstorm.communication.internodes.CommunicationClient;
import com.lenss.mstorm.communication.internodes.CommunicationServer;
import com.lenss.mstorm.communication.internodes.Dispatcher;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.executor.Executor;
import com.lenss.mstorm.executor.ExecutorManager;
import com.lenss.mstorm.status.StatusReporter;
import com.lenss.mstorm.topology.BTask;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.MyPair;
import com.lenss.mstorm.utils.Serialization;
import com.lenss.mstorm.zookeeper.Assignment;
import com.sun.xml.internal.ws.resources.ManagementMessages;

public class ComputingNode implements Runnable {
	/// LOGGER
	private final String TAG="ComputingNode";
	Logger logger = Logger.getLogger(TAG);

	//private static String jarDirectory= System.getProperty("user.home") + File.separator + "EdgeStorm" + File.separator;
	//private static String jarDirectory= "/";
	private static String jarDirectory= ""; 
	//// EXECUTORS
	private ExecutorManager mExecutorManager;
	private LinkedBlockingDeque<Runnable> mExecutorQueue;
	private final int NUMBER_OF_CORE_EXECUTORS = 8;
	private final int NUMBER_OF_MAX_EXECUTORS = 16;
	private final int KEEP_ALIVE_TIME = 60;
	private final TimeUnit KEEP_ALIVE_TIME_UNIT = TimeUnit.SECONDS;

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
	private volatile int allConnected=0;

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

		localTasks = assignment.getNode2Tasks().get(MStormWorker.GUID);
		if (localTasks != null) {
			for (int i = 0; i < localTasks.size(); i++) {
				int taskId = localTasks.get(i);
				MessageQueues.addQueuesForTask(taskId);
			}
		}

		// setup server and client for communication with other nodes
		if (mServer != null) {
			mServer.release();
		}
		mServer = new CommunicationServer();
		mServer.setup();

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

		while(allConnected==0){     // wait until all workers are connected
			try {
				Thread.sleep(5);
				logger.info("Waiting workers to connect with each other ... ");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if(allConnected==-1){
			logger.error("Some connections between workers failed ... ");
			Supervisor.mHandler.handleMessage(Supervisor.Message_LOG, "Workers cannot establish connections, please turn off and try again ...");
		} else{
			logger.info("All connections among workers succeed ... ");
		}

		// execute tasks assigned to this node
		String jarFileName = assignment.getApk().split("\\.")[0] + ".jar";
		String jarFilePath = jarDirectory+jarFileName;
		System.out.println(jarFilePath);
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
				HashMap<Integer, String> task2Component = assignment.getTask2Component();
				String sourceAddr  = assignment.getSourceAddr();  // GUID addr
				String sourceIP  = GNSServiceHelper.getIPInUseByGUID(sourceAddr);
				for (int i = 0; i < localTasks.size(); i++) {
					int taskID = localTasks.get(i);
					String component = task2Component.get(taskID);
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

	public void stop() {
		// stop client and server for communication
        if (mClient != null) {
            mClient.release();
            mClient = null;
        }
        if (mServer != null) {
            mServer.release();
            mServer = null;
        }

        // stop the executors
        List<Runnable> threadsInExecutor = mExecutorManager.shutdownNow();
        for(Runnable thread: threadsInExecutor){
            if(thread instanceof Executor)
                ((Executor) thread).stop();
        }

        try {
            System.out.println("Wait all threads in executor pool to stop ... ");
            Thread.sleep(100);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        // stop packet dispatcher
        dispatcher.stop();
        // stop status reporter
        reporterThread.interrupt();
        // clear computation status
        MessageQueues.removeTaskQueues();
        // clear channel status
        ChannelManager.releaseChannelsToRemote();
	}
	
	public void connectTasks() {
		List<String> assignNodes = assignment.getAssginedNodes();
		int numOfNodes = assignNodes.size();
		String localAddr = MStormWorker.GUID;
		int indexOfLocalNode;
		if ((indexOfLocalNode = assignNodes.indexOf(localAddr)) != -1){
			int[][] node2NodeConnection = assignment.getNode2NodeConnection();
			for (int i = 0; i < numOfNodes; i++) {
				if (node2NodeConnection[indexOfLocalNode][i] == 1) {
					String remoteGUID = assignNodes.get(i);
					ChannelFuture cf = mClient.connectByGUID(remoteGUID);
					cf.awaitUninterruptibly();
					Channel currentChannel = cf.getChannel();
					if (cf.isSuccess() && currentChannel != null && currentChannel.isConnected()) {
						String msg = "A connection from " + currentChannel.getLocalAddress().toString() + " to " + currentChannel.getRemoteAddress().toString() + " succeeds ... ";
						logger.debug(msg);
					} else {
						if (currentChannel != null) {
							currentChannel.close();
						}
						allConnected = -1;
					}
				}
			}
		}
		allConnected = 1;
	}

	public static int getPauseOrContinue(){return pauseOrContinue;}

	public static void setPauseOrContinue(int p){pauseOrContinue = p;}

	// get assignment
	public static Assignment getAssignment() {
		return assignment;
	}

	// get topology
	public static Topology getTopology(){
		return topology;
	}

	// get tasks assigned to this computing node
	public static ArrayList<Integer> getLocalTasks(String localAddress, Assignment assign) {
		HashMap<String, ArrayList<Integer>> node2Tasks = assign.getNode2Tasks();
		return node2Tasks.get(localAddress);
	}

}