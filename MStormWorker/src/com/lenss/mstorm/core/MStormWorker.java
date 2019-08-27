package com.lenss.mstorm.core;

import org.apache.zookeeper.Shell.ExitCodeException;
import org.eclipse.jetty.util.statistic.SampleStatistic;

import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.Helper;


public class MStormWorker{     
    public static final int SESSION_TIMEOUT = 10000;
    public static String ZK_ADDRESS_IP;
    
    public static String MASTER_NODE_GUID;
    public static String MASTER_NODE_IP;
    public static final int MASTER_PORT = 12016;
    
    public static String GUID;
    public static String localAddress;
    
    public static String isPublicOrPrivate;
    
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
		GUID = GNSServiceHelper.getOwnGUID();
		isPublicOrPrivate = "1";
		localAddress = Helper.getIPAddress(true);
		
		MASTER_NODE_IP = GNSServiceHelper.getMasterNodeIPInUse();
		if (MASTER_NODE_IP == null){
            System.out.println("MStorm Master Unregistered OR EdgeKeeper unreachable!\nPlease check them and restart this APP.");
        }
		ZK_ADDRESS_IP = MASTER_NODE_IP;
		
		MASTER_NODE_GUID = GNSServiceHelper.getMasterNodeGUID();
		
		mSupervisor = new Supervisor();
		mSupervisor.onStartCommand();
	}
}