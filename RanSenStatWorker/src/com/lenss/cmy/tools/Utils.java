package com.lenss.cmy.tools;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static Random rand = new Random();

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void highPrecisionSleep(long millis, int nanos){
        try {
            Thread.sleep(millis,nanos);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getFstWord(String sentence){
        String arr[] = sentence.split("@", 2);
        String firstWord = arr[0];
        return firstWord;
    }

    public static String getRestString(String sentence){
        String arr[] = sentence.split("@", 2);
        String restString = arr[1];
        return restString;
    }

    public static Boolean isZeroByteArray(byte[] array) {
        for (byte b : array) {
            if (b != (byte) 0) {
                return false;
            }
        }
        return true;
    }

    public static void clearByteArray(byte[] array) {
        Arrays.fill(array, (byte) 0);
    }

    public static byte[] getUsefulBytes(byte[] array){
        int index = 0;
        for (byte b : array) {
            if (b != (byte) 0) {
                index++;
            }
        }
        byte[] usefulBytes = new byte[index];
        System.arraycopy(array, 0, usefulBytes, 0, index);
        return usefulBytes;
    }

    public static void fakeExecutionTime(int loopTimes){

        //// constant input
        double randomVarPercent = 0.0;

        //// unified random workload
        //randomVarPercent = (rand.nextDouble() * 2.0 - 1.0) * 0.5;

        //// gaussian random workload
/*        randomVarPercent = rand.nextGaussian()*(1.5-0.5)/6 + (0.5 + 1.5)/2;
        while (!((randomVarPercent >= 0.5) && (randomVarPercent <= 1.5))) {
            randomVarPercent = rand.nextGaussian()*(1.5-0.5)/6 + (0.5 + 1.5)/2;
        }*/

        //// 2-8 pareto random
        /*double random = rand.nextDouble();
        if(random <= 0.2)
            randomVarPercent = 2.0;
        else
            randomVarPercent = 1.0;*/

        for (int i=0;i<loopTimes;i++){
            int innerLoopTimes = (int) (100000 * (1+randomVarPercent));
            for(int j=0; j<innerLoopTimes;j++)
            {
                Math.tan(20);
            }
        }
    }

    public static boolean isNumber(String str) {
        Pattern p = Pattern.compile("^[0-9]+$");
        Matcher m = p.matcher(str);
        return m.matches();
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
}
