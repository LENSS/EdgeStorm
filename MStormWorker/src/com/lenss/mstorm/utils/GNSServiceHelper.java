package com.lenss.mstorm.utils;

import org.apache.log4j.Logger;
import org.junit.internal.requests.SortingRequest;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.tamu.cse.lenss.edgeKeeper.client.*;

/**
 * Created by cmy on 4/15/19.
 */

public class GNSServiceHelper {

	public static final String TAG = "GNSServiceHelper";
	public static final Logger logger = Logger.getLogger(TAG);
	
	public static String getMasterNodeGUID(){
		String masterGUID = null;
        List<String> masterGUIDs = EKClient.getPeerGUIDs("MStorm", "master");

        ExecutorService executor = Executors.newFixedThreadPool(masterGUIDs.size());
        List<ValidGUID> validGUIDs = new ArrayList<>();
        for (String candidateGUID: masterGUIDs){
            ValidGUID validGuid = new ValidGUID(candidateGUID);
            validGUIDs.add(validGuid);
        }
        try {
            masterGUID = executor.invokeAny(validGUIDs);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            logger.error("No MStorm Master Usable!");
        } finally{
            executor.shutdownNow();
        }
        return masterGUID;
	}
	
    static class ValidGUID implements Callable<String>{
        public String GUID;
        public ValidGUID(String guid) {GUID = guid;}
        public String call() throws Exception{
            if(getIPInUseByGUID(GUID)!=null)
                return GUID;
            else
                return null;
        }
    }

	public static String getIPInUseByGUID(String GUID){
		String IPInUse = null;
		List<String> IPs = EKClient.getIPbyGUID(GUID);
		if(IPs.size()!=0) {
			try {
				List<String> siteLocalIPs = getSiteLocal(IPs);
				if (siteLocalIPs.size() != 0) {
					IPInUse = getReachable(siteLocalIPs);
					if(IPInUse == null) {
						IPInUse = getReachable(IPs);
					}
				} else {
					IPInUse = getReachable(IPs);
				} 
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return IPInUse;
	}

	public static List<String> getSiteLocal(List<String> hostIPs) throws Exception {
		List<String> result = new ArrayList<>();
		for (String hostIP: hostIPs){
			InetAddress tmp = InetAddress.getByName(hostIP);
			if (tmp.isSiteLocalAddress())
				result.add(hostIP);
		}
		return result;
	}

	public static String getReachable(List<String> hostIPs) {
		ExecutorService executor = Executors.newFixedThreadPool(hostIPs.size());
		String result = null;
		List<PingRemoteAddress> pings = new ArrayList<>();

		for (String hostIP: hostIPs){
			PingRemoteAddress ping = new PingRemoteAddress(hostIP);
			pings.add(ping);
		}

		try {
			result = executor.invokeAny(pings);
		} catch (ExecutionException | InterruptedException e) {
			e.printStackTrace();
		} finally{
			executor.shutdownNow();
		}
		return result;
	}   

	static class PingRemoteAddress implements Callable<String>{
		public String remoteHostIp;
		public PingRemoteAddress(String HostIp){
			remoteHostIp = HostIp;
		}
		public String call() throws Exception{
			if(InetAddress.getByName(remoteHostIp).isReachable(4000))
				return remoteHostIp;
			else
				return null;
		}
	}

	public static String getOwnGUID(){
		return EKClient.getOwnGuid();
	}

	public static String getGUIDByIP(String IP){
		List<String> GUIDs = EKClient.getGUIDbyIP(IP);
		if(GUIDs.size() == 1){
			return GUIDs.get(0);
		} else {
			if(GUIDs.size()>1) {
				logger.error("Multiple GUIDs for this IP");
			} else {
				logger.error("GNS service unreachable");
			}
			return null;
		}
	}
	
	public static String getZookeeperIP() {
	    return EKClient.getZooKeeperConnectionString();
	}
}
