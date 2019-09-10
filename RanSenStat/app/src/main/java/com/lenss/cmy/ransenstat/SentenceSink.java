package com.lenss.cmy.ransenstat;

import android.net.LocalSocket;
import android.net.LocalSocketAddress;
import android.util.Pair;

import com.google.gson.annotations.Expose;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.topology.Processor;
import com.lenss.mstorm.utils.Serialization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import tools.Utils;

/**
 * Created by cmy on 8/23/19.
 */

public class SentenceSink extends Processor {
    @Expose
    private final String LOCAL_RESULT_ADDRESS = "com.android.cmy.ransenstat.result";
    @Expose
    private int workload_SenSink;

    private ResultLocalClientThread mClient;
    //private ResultClientThread mClient;

    public void setWorkLoad(int workLoad) {
        workload_SenSink = workLoad;
    }

    @Override
    public void prepare() {
        mClient = new ResultLocalClientThread();
        new Thread(mClient).start();
//        mClient = new ResultClientThread();
//        new Thread(mClient).start();

    }

    @Override
    public void execute() {
        int taskID = getTaskID();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                InternodePacket pkt = MessageQueues.retrieveIncomingQueue(taskID);
                if (pkt != null) {
                    Utils.fakeExecutionTime(workload_SenSink);
                    String component = "END";
                    pkt.type = InternodePacket.TYPE_DATA;
                    pkt.fromTask = taskID;
                    MessageQueues.emit(pkt, taskID, component);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void postExecute() {
        mClient.stop();
    }

    public class ResultLocalClientThread implements Runnable {             // move this thread to mStorm as future work
        LocalSocket localClient;
        DataOutputStream os;
        boolean connected = false;

        public void run() {
            localClient = new LocalSocket();

            try {
                localClient.connect(new LocalSocketAddress(LOCAL_RESULT_ADDRESS));
                if (localClient.isConnected()) {
                    connected = true;
                }
                os = new DataOutputStream(localClient.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("************** Get connected to [result server]****************");

            while (connected) {
                Pair<String, InternodePacket> data = MessageQueues.retrieveResultQueue(getTaskID());
                if (data != null) {
                    try {
                        InternodePacket pkt = data.second;
                        String strPkt = Serialization.Serialize(pkt);
                        byte[] pktByte = strPkt.getBytes("UTF-8");
                        os.write(pktByte);
                        os.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                        connected = false;
                    }
                }
            }
        }

        public void stop() {
            connected = false;
            if (!localClient.isClosed()) {
                try {
                    localClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("The ResultClientThread has stopped ... ");
            }
        }
    }

    public class ResultClientThread implements Runnable {
        Socket client;
        DataOutputStream os;
        boolean connected = false;

        public void run() {
            try {
                InetAddress serverAddr = InetAddress.getByName(getSourceIP());
                client = new Socket(serverAddr, 7654);
                if (client.isConnected()) {
                    connected = true;
                }
                os = new DataOutputStream(client.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("************** Get connected to [result server]****************");

            while (!Thread.currentThread().isInterrupted() && connected) {
                Pair<String, InternodePacket> data = MessageQueues.retrieveResultQueue(getTaskID());
                if (data != null) {
                    try {
                        InternodePacket pkt = data.second;
                        String strPkt = Serialization.Serialize(pkt);
                        byte[] pktByte = strPkt.getBytes("UTF-8");
                        os.write(pktByte);
                        os.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                        connected = false;
                    }
                }
            }
        }

        public void stop() {
            connected = false;
            if (!client.isClosed()) {
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("The ResultClientThread has stopped ... ");
            }
        }
    }
}
