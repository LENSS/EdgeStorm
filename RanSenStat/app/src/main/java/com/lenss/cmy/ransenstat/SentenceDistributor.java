package com.lenss.cmy.ransenstat;

import android.net.LocalServerSocket;
import android.net.LocalSocket;
import android.net.LocalSocketAddress;
import android.util.Pair;

import com.google.gson.annotations.Expose;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.topology.Distributor;
import com.lenss.mstorm.utils.Serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import tools.Utils;

/**
 * Created by cmy on 6/23/16.
 */
public class SentenceDistributor extends Distributor {
    @Expose
    private final String LOCAL_ADDRESS = "com.android.cmy.ransenstat";
    @Expose
    private int workload_senDistributor;

    PullStreamThreadLocal pullStream;
    //PullStreamThread pullStream;

    public void setWorkload(int workload) {
        workload_senDistributor = workload;
    }

    @Override
    public void prepare() {
        pullStream = new PullStreamThreadLocal();
        new Thread(pullStream).start();
        //pullStream = new PullStreamThread();
        //new Thread(pullStream).start();
    }

    @Override
    public void execute() {
        int taskID = getTaskID();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                InternodePacket pkt = MessageQueues.retrieveIncomingQueue(taskID);
                if (pkt != null) {
                    Utils.fakeExecutionTime(workload_senDistributor);
                    String component = TopicTagger.class.getName();
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
        pullStream.stop();
    }

    class PullStreamThreadLocal implements Runnable {
        private LocalSocket localClient;
        private DataInputStream input;
        private boolean connected;

        public void run() {
            localClient = new LocalSocket();
            try {
                localClient.connect(new LocalSocketAddress(LOCAL_ADDRESS));
                if (localClient.isConnected()) {
                    connected = true;
                }
                input = new DataInputStream(localClient.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("************** Get connected to [stream server]****************");

            while (connected && ComputingNode.getPauseOrContinue() == false) {
                try {
                    int count = input.available();
                    if (count > 0) {
                        byte[] pktByte = new byte[count];
                        input.read(pktByte);
                        String serializedPkt = new String(pktByte, "UTF-8");
                        InternodePacket pkt = (InternodePacket) Serialization.Deserialize(serializedPkt, InternodePacket.class);
                        MessageQueues.collect(getTaskID(), pkt);
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    connected = false;
                }
            }

            if (ComputingNode.getPauseOrContinue() != false) {
                try {
                    localClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
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
            }
        }
    }

    class PullStreamThread implements Runnable {
        private Socket client;
        private DataInputStream input;
        private boolean connected;

        public void run() {
            try {
                InetAddress serverAddr = InetAddress.getByName(getSourceIP());
                client = new Socket(serverAddr, 8999);
                if (client.isConnected()) {
                    connected = true;
                }
                input = new DataInputStream(client.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("************** Get connected to [stream server]****************");

            while (!Thread.currentThread().isInterrupted() && connected) {
                try {
                    int count = input.available();
                    if (count > 0) {
                        byte[] pktByte = new byte[count];
                        input.read(pktByte);
                        String serializedPkt = new String(pktByte, "UTF-8");
                        InternodePacket pkt = (InternodePacket) Serialization.Deserialize(serializedPkt, InternodePacket.class);
                        MessageQueues.collect(getTaskID(), pkt);
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    connected = false;
                }
            }

            if (connected != false) {
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void stop() {
            if (!client.isClosed()) {
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

