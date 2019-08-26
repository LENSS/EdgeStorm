package com.lenss.mstorm.core;

import com.lenss.mstorm.communication.masternode.MasterNodeClient;
import com.lenss.mstorm.utils.Helper;
import com.sun.org.apache.xml.internal.resolver.helpers.PublicId;


public class MStormWorker{
    public static final int SESSION_TIMEOUT = 5000;
    public static String MASTER_NODE = "192.168.0.5";
    public static String ZK_ADDRESS = "192.168.0.5";
    public static String localAddress = "192.168.0.5";
    
    public static Supervisor mSupervisor;
	
	public static class MStormWorkerHolder{
		public static final MStormWorker mstormWorker = new MStormWorker();
	}
	
	public static MStormWorker getInstance() {
		return MStormWorkerHolder.mstormWorker;
	} 
	
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("USAGE: MasterNode_IP(Zookeeper_IP)");
			System.exit(2);
		}
		MStormWorker mStormWorker = MStormWorker.getInstance();
		mStormWorker.setup(args[0]);
	}
	
	public void setup(String masterNodeIP) {
		MASTER_NODE = masterNodeIP;
		ZK_ADDRESS = MASTER_NODE;
		//localAddress = Helper.getIPv4Address(true);
		
		mSupervisor = new Supervisor();
		mSupervisor.onStartCommand();
	}
}