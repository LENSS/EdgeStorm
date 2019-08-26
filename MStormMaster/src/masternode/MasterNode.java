package masternode;

import java.io.File;

import java.io.IOException;
import java.net.MalformedURLException;
import nimbusscheduler.NimbusScheduler;
import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.KeeperException;
import zookeeper.ZookeeperClient;
import communication.CommunicationServer;
import edu.tamu.cse.lenss.gnsService.client.*;

public class MasterNode {
	public ZookeeperClient mZkClient;
	public CommunicationServer mServer;
	public NimbusScheduler mNimbusScheduler;
	public GnsServiceClient gnsClient;

	public static class MasterNodeHolder{
		public static final MasterNode instance = new MasterNode();
	}
	
	public static MasterNode getInstance(){
		return MasterNodeHolder.instance;
	}
	
	public void setup(String portNum){
		// establish a zooKeeper client
		try {
			mZkClient = new ZookeeperClient(portNum);
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
		gnsClient = new GnsServiceClient();
		if(gnsClient.addService("MStorm", "master")) {
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
		masterNode.setup(args[0]);
	}
}
