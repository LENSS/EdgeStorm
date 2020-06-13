package com.lenss.mstorm.utils;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class MDFSClient {
    /// LOGGER
    private static final String TAG="MDFSClient";
    private static Logger logger = Logger.getLogger(TAG);

    //constant static fields
    private static String LOCALFILEPATH = "LOCALFILEPATH";
    private static String MDFSFILEPATH = "MDFSFILEPATH";
    private static int MDFS_API_PORT = 5153;

    //put function
    public static void put(String filepathWithName, String mdfsDirectory){

        //make a MDFS request packet
        JSONObject reqPacket = new JSONObject();

        //add parameters
        try {
            reqPacket.put(LOCALFILEPATH, filepathWithName);
            reqPacket.put(MDFSFILEPATH, mdfsDirectory);
        }catch (JSONException e){
            e.printStackTrace();
        }

        //make string and then make byte[]
        byte[] data = reqPacket.toString().getBytes();

        try{
            //init socket and ip
            DatagramSocket ds = new DatagramSocket();
            InetAddress ip = InetAddress.getByName("127.0.0.1");

            //make UDP packet
            DatagramPacket DpSend = new DatagramPacket(data, data.length, ip, MDFS_API_PORT);

            //send
            ds.send(DpSend);

            logger.info("request sent to MDFS");

        }catch(IOException e){
            e.printStackTrace();
        }
    }

}
