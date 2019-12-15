package com.lenss.mstorm.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.log4j.Logger;

import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.Helper;


public class MStormWorker{     
	Logger logger = Logger.getLogger("MStormWorker");
	
	public static final int SESSION_TIMEOUT = 10000;
	public static String ZK_ADDRESS_IP;

	public static String MASTER_NODE_GUID;
	public static String MASTER_NODE_IP;
	public static final int MASTER_PORT = 12016;

	public static String GUID;
	public static String localAddress;

	public static String isPublicOrPrivate;
   
    public static double availability = 1.0;

	public static Supervisor mSupervisor;

	public static class MStormWorkerHolder{
		public static final MStormWorker mstormWorker = new MStormWorker();
	}

	public static MStormWorker getInstance() {
		return MStormWorkerHolder.mstormWorker;
	} 

	public static void main(String[] args) {
		//		if (args.length < 1) {
			//			System.err.println("USAGE: MasterNode_IP(Zookeeper_IP)");
		//			System.exit(2);
		//		}
		
		try {
			System.setProperty("log4j.configuration", new File(".", File.separatorChar+"log4j.properties").toURL().toString());
			//System.setProperty("java.util.logging.config.file", new File(".", File.separatorChar+"logging.properties").toURL().toString());
		} catch (MalformedURLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		MStormWorker mStormWorker = MStormWorker.getInstance();
		mStormWorker.setup();

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				exit();
			}
		}, "MStormWorker shutdown"));
	}

	public static void exit() {
		if(mSupervisor!=null)
			mSupervisor.onDestroy();
	}

	public void setup() {
		// Get own GUID and IP
		GUID = GNSServiceHelper.getOwnGUID();
		if(GUID == null) {
			logger.info("EdgeKeeper unreachable!");
			System.exit(-1);
		}
		localAddress = Helper.getIPAddress(true);

		// Get Master Node GUID and IP
		MASTER_NODE_GUID = GNSServiceHelper.getMasterNodeGUID();
		if (MASTER_NODE_GUID == null){
			logger.info("MStorm Master Unregistered!");
			System.exit(-1);
		}
		logger.info("The Master GUID is: " + MASTER_NODE_GUID);
		
		MASTER_NODE_IP = GNSServiceHelper.getIPInUseByGUID(MASTER_NODE_GUID);
		if (MASTER_NODE_IP == null){
			logger.info("MStorm Master unreachable!");
			System.exit(-1);
		}
		logger.info("The Master IP is: " + MASTER_NODE_IP);

		// Get ZOOKEEPER IP
		//ZK_ADDRESS_IP = GNSServiceHelper.getZookeeperIP();

		// Get Configuration, public(1) or private(0)
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader("MSW.conf"));
			reader.readLine();
			isPublicOrPrivate = reader.readLine().split("\\=")[1];
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(reader!=null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		// Start Supervisor
		mSupervisor = new Supervisor();
		mSupervisor.onStartCommand();
	}
}