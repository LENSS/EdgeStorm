package masternode;

import java.io.File;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashSet;

import nimbusscheduler.NimbusScheduler;
import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.KeeperException;

import cluster.Cluster;
import zookeeper.ZookeeperClient;
import communication.CommunicationServer;
import edu.tamu.cse.lenss.edgeKeeper.client.EKClient;

public class MasterNode {
	public static ZookeeperClient mZkClient;
	public CommunicationServer mServer;
	public NimbusScheduler mNimbusScheduler;

	public static class MasterNodeHolder{
		public static final MasterNode instance = new MasterNode();
	}
	
	public static MasterNode getInstance(){
		return MasterNodeHolder.instance;
	}
	
	public void setup(){
		// establish a zooKeeper client
		try {
			mZkClient = new ZookeeperClient(EKClient.getZooKeeperConnectionString());
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		new Thread(mZkClient).start();
		
		// establish a communication server
		mServer = new CommunicationServer();
		mServer.setup();
		
		// establish a Nimbus scheduler
		mNimbusScheduler= new NimbusScheduler();
		
		// register as a service to GNS server
		if(EKClient.addService("MStorm", "master")) {
			System.out.println("MStorm master successfully register to GNS server ... ");
		} else {
			System.out.println("MStorm master can NOT register to GNS server ... ");
		}
	}
	
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("USAGE: MasterNode Zookeeper_Address");
			System.exit(2);
		}
		
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
		
		EKClient.removeService("MStorm");
	}
}
