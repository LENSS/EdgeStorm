package com.lenss.mstorm.utils;

import com.lenss.mstorm.core.MStorm;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

        if(masterGUIDs.size() == 0){
            masterGUID = null;
        } else {
            ExecutorService executor = Executors.newFixedThreadPool(masterGUIDs.size());
            List<ValidingMasterGUID> potentialGUIDs = new ArrayList<>();
            for (String potentialGUID : masterGUIDs) {
                ValidingMasterGUID validGuid = new ValidingMasterGUID(potentialGUID);
                potentialGUIDs.add(validGuid);
            }

            // using invokeAll
            List<Future<String>> candidateGUIDs;
            try {
                candidateGUIDs = executor.invokeAll(potentialGUIDs);
                if (candidateGUIDs != null && candidateGUIDs.size()!=0) {
                    for(Future<String> guid: candidateGUIDs) {
                        masterGUID = guid.get();
                        if(masterGUID!=null)
                            break;
                    }
                } else {
                    logger.error("NO candidate Master GUID is reachable");
                }
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
                logger.error("No MStorm Master Usable!");
            } finally {
                executor.shutdownNow();
            }

            // using invokeAny
//        try {
//            masterGUID = executor.invokeAny(potentialGUIDs);
//        } catch (ExecutionException | InterruptedException e) {
//            e.printStackTrace();
//            logger.error("No MStorm Master Usable!");
//        } finally{
//            executor.shutdownNow();
//        }

        }
        return masterGUID;
    }

    static class ValidingMasterGUID implements Callable<String>{
        public String GUID;
        public ValidingMasterGUID(String guid) {GUID = guid;}
        public String call(){
            String ipAddr = getIPInUseByGUID(GUID);
            if(ipAddr!=null){
                Boolean isConnectable;
                Socket RTSPSocket = null;
                try {
                    InetAddress ServerIPAddr = InetAddress.getByName(ipAddr);
                    RTSPSocket = new Socket(ServerIPAddr, MStorm.MASTER_PORT);
                    isConnectable = true;
                } catch (UnknownHostException e) {
                    logger.error("Could not find host ... ");
                    e.printStackTrace();
                    isConnectable = false;
                } catch (IOException e) {
                    logger.error("Could not establish socket ... ");
                    e.printStackTrace();
                    isConnectable = false;
                } finally {
                    // close the socket
                    if(RTSPSocket!=null && RTSPSocket.isConnected()){
                        try {
                            RTSPSocket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                if(isConnectable)
                    return GUID;
                else
                    return null;
            } else {
                return null;
            }
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
                        IPs.removeAll(siteLocalIPs);
                        if(IPs.size()!=0) {
                            IPInUse = getReachable(IPs);
                        }
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
        String result = null;
        ExecutorService executor = Executors.newFixedThreadPool(hostIPs.size());
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
            if(InetAddress.getByName(remoteHostIp).isReachable(3000))
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
