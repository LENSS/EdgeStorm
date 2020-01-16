package masternode;

import java.io.File;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashSet;

import nimbusscheduler.NimbusScheduler;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import cluster.Cluster;
import zookeeper.ZookeeperClient;
import communication.CommunicationServer;
import edu.tamu.cse.lenss.edgeKeeper.client.EKClient;

public class MasterNode {
	public static ZookeeperClient mZkClient;
	public static String zooKeeperConnectionString;
	static Logger logger = Logger.getLogger("MasterNode");

	public CommunicationServer mServer;
	public NimbusScheduler mNimbusScheduler;


	public static class MasterNodeHolder{
		public static final MasterNode instance = new MasterNode();
	}
	
	public static MasterNode getInstance(){
		return MasterNodeHolder.instance;
	}
	
	public void setup(){
		// get zookeeper connection string
		while ((zooKeeperConnectionString = EKClient.getZooKeeperConnectionString()) == null) {
			logger.error("Can NOT get Zookeeper connection string, try again to get it after 1s ... ");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		logger.info("\n\n"+" ===================================== MStormMaster successfully gets the Zookeeper connection string! ====================================\n");
		System.out.println("MStormMaster successfully gets the Zookeeper connection string!");
		
		// establish a zooKeeper client for MStormMaster
		try {	
			mZkClient = new ZookeeperClient(zooKeeperConnectionString);
			logger.info("Start a Zookeeper client ...");
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		new Thread(mZkClient).start();
		
		// register as a service to GNS server
		while(!EKClient.addService("MStorm", "master")) {
			logger.error("MStormMaster can NOT register to GNS server as a service, try again to get it after 1s ... ");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		logger.info("\n\n"+" ===================================== MStormMaster successfully registers to GNS server as a service ====================================\n");
		System.out.println("MStormMaster successfully registers to GNS server as a service!");
		
		// establish a Nimbus scheduler
		mNimbusScheduler= new NimbusScheduler();
		
		// establish a communication server to receive request from clients
		mServer = new CommunicationServer();
		mServer.setup();
        logger.info("\n\n"+" ===================================== Communication Server at MStormMaster Starts, waiting requests from clients!====================================\n");
        System.out.println("Communication Server at MStormMaster Starts, waiting requests from clients!");
	}
	
	public static void main(String[] args) {
		//BasicConfigurator.configure();		
		try {
			System.setProperty("log4j.configuration", new File(".", File.separatorChar+"log4j.properties").toURL().toString());
			//System.setProperty("java.util.logging.config.file", new File(".", File.separatorChar+"logging.properties").toURL().toString());
		} catch (MalformedURLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		MasterNode masterNode = MasterNode.getInstance();
		masterNode.setup();
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
	        public void run() {
	        	exit();
	        }
	    }, "MStormMaster shutdown"));	
	}
	
	public static void exit() {
		// unregister clusters managed by this master in zookeeper
		for(Cluster cluster: Cluster.clusters.values()) {
			if(mZkClient!=null){
				try {
					mZkClient.deleteCluster(cluster);
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		while(!EKClient.removeService("MStorm")) {
			logger.error("MStormMaster can NOT remove as a service from GNS server, try again to get it after 1s ... ");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		logger.info("\n\n"+" ===================================== MStormMaster successfully remove as a service from GNS server ====================================\n");
		System.out.println("MStormMaster successfully remove as a service from GNS server!");
	}
}
