package com.lenss.mstorm.utils;

import android.os.Environment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.nio.ByteBuffer;

public class Helper {

    public  static String getHostName(String addr) {
        String hostname=addr.substring(0, addr.indexOf(':'));
        return hostname;
    }

    public static int getPort(String addr) {
        int port = Integer.parseInt(addr.substring(addr.indexOf(':') + 1, addr.length()));
        return port;
    }

    public static int randInt(int min, int max) {   //[min, max)
        Random rand = new Random();
        int randomNum = rand.nextInt((max - min)) + min;
        return randomNum;
    }

    public static double randDouble(double min, double max){
        Random rand = new Random();
        double randomNum = rand.nextDouble()*(max-min)+min;
        return randomNum;
    }

    public static double doubleRandomInclusive(double max, double min) {
        double r = Math.random();
        if (r < 0.5) {
            return ((1 - Math.random()) * (max - min) + min);
        }
        return (Math.random() * (max - min) + min);
    }

    public static byte[] doubleArraytoByteArray(double[] from) {
        int times = Double.SIZE / Byte.SIZE;
        byte[] output = new byte[from.length*times];
        for(int i=0;i<from.length;i++){
            ByteBuffer.wrap(output, i*times, times).putDouble(from[i]);
        }
        return output;
    }

    public static double[] byteArraytoDoubleArray(byte[] byteArray){
        int times = Double.SIZE / Byte.SIZE;
        int num = byteArray.length/times;
        double[] doubles = new double[num];
        for(int i=0;i<num;i++){
            doubles[i] = ByteBuffer.wrap(byteArray, i*times, times).getDouble();
        }
        return doubles;
    }

    public static int bytesToInt(byte[] bytes){
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static byte[] IntToBytes(int x) {
        return ByteBuffer.allocate(4).putInt(x).array();
    }

    private ArrayList<String> getDiff(List<String> moreNodes, List<String> lessNodes) {
        HashSet<String> temp=new HashSet<String>();
        for(int i=0;i<lessNodes.size();i++)
        {
            temp.add(lessNodes.get(i));
        }
        ArrayList<String> result=new  ArrayList<String>();
        for(int i=0;i<moreNodes.size();i++)
        {
            if(!temp.contains(moreNodes.get(i)))
            {
                result.add(moreNodes.get(i));
            }
        }
        return result;
    }

    public static String getIPAddress(boolean useIPv4) {
        try {
            List<NetworkInterface> interfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
            for (NetworkInterface intf : interfaces) {
                List<InetAddress> addrs = Collections.list(intf.getInetAddresses());
                for (InetAddress addr : addrs) {
                    if (!addr.isLoopbackAddress()) {
                        String sAddr = addr.getHostAddress();
                        //boolean isIPv4 = InetAddressUtils.isIPv4Address(sAddr);
                        boolean isIPv4 = sAddr.indexOf(':')<0;

                        if (useIPv4) {
                            if (isIPv4)
                                return sAddr;
                        } else {
                            if (!isIPv4) {
                                int delim = sAddr.indexOf('%'); // drop ip6 zone suffix
                                return delim<0 ? sAddr.toUpperCase() : sAddr.substring(0, delim).toUpperCase();
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) { } // for now eat exceptions
        return "";
    }

    // For container
    public static String getIPAddress() {
        File sdcard = Environment.getExternalStorageDirectory();
        File file = new File(sdcard,"ip.txt");
        String ipAddress = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            ipAddress = br.readLine();
            br.close();
        }
        catch (IOException e) {
            //You'll need to add proper error handling here
        }
        return ipAddress;
    }
}
