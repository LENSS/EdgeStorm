package com.lenss.mstorm.topology;

import android.content.Context;
import android.os.AsyncTask;
import com.lenss.mstorm.communication.masternode.FileServer;
import com.lenss.mstorm.communication.masternode.MasterNodeClient;
import com.lenss.mstorm.communication.masternode.Reply;
import com.lenss.mstorm.communication.masternode.Request;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.Serialization;
import java.net.InetSocketAddress;

/**
 * StormSubmitter: used as API for user to submit topology to mStorm
 * */

public class StormSubmitter {
    private MasterNodeClient masterNodeClient=null;
    private FileServer fileServer=null;
    private String masterNodeGUID;
    private String ownGUID;
    private String apkFileDirectory;
    private Reply reply = null;

    public StormSubmitter(Context mContext, String apkDirectory){
        this.masterNodeGUID = GNSServiceHelper.getMasterNodeGUID();
        this.ownGUID = GNSServiceHelper.getOwnGUID() ;
        this.apkFileDirectory = apkDirectory;

        //// can be commented out for real exercise !!!!!!!!
//        // set up file server on mobile phone to let Nimbus get apk file
//        fileServer = new FileServer(apkFileDirectory);
//        fileServer.setup();
        //// can be commented out for real exercise !!!!!!!!


        // let user app connect to Nimbus
        if(masterNodeClient==null)
        {
            masterNodeClient=new MasterNodeClient(masterNodeGUID);
            masterNodeClient.connect();
        }
    }

    public void submitTopology(String apkFileName, Topology topology){
        Request req=new Request();
        req.setReqType(Request.TOPOLOGY);
        req.setGUID(ownGUID);
        req.setContent(Serialization.Serialize(topology));
        req.setFileName(apkFileName);

        // waiting for connection
        while(!masterNodeClient.isConnected()){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        masterNodeClient.sendRequest(req);

        // waiting for reply
        while((reply=masterNodeClient.getReply())==null){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        masterNodeClient.close();
    }

    public Reply getReply(){
        return reply;
    }
}
