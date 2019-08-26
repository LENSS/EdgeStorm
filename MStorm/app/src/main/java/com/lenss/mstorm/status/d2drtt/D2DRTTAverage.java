package com.lenss.mstorm.status.d2drtt;

import com.lenss.mstorm.status.StatusReporter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by cmy on 1/13/17.
 */
public class D2DRTTAverage implements Runnable {

    private final int NUMBER_OF_PACKTETS = 10;
    private String ipAddress;

    public D2DRTTAverage(String addr){
        ipAddress = addr;
    }

    /**
        Returns the latency to a given server in mili-seconds by issuing a ping command.
        system will issue NUMBER_OF_PACKTETS ICMP Echo Request packet each having size of 56 bytes
        every second, and returns the avg latency of them.
        Returns 0 when there is no connection
    */
    @Override
    public void run(){
        double avgRtt = 1000000.0;  // just big enough

        //// Comment out for March Exercise - LTE Case
//        String pingCommand = "/system/bin/ping -c " + NUMBER_OF_PACKTETS + " " + ipAddress;
//        String inputLine = "";
//        try {
//            // execute the command on the environment interface
//            Process process = Runtime.getRuntime().exec(pingCommand);
//            // gets the input stream to get the output of the executed command
//            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//
//            inputLine = bufferedReader.readLine();
//            while ((inputLine != null)) {
//                if (inputLine.length() > 0 && inputLine.contains("avg")) {  // when we get to the last line of executed ping command
//                    break;
//                }
//                inputLine = bufferedReader.readLine();
//            }
//        }
//        catch (IOException e){
//            e.printStackTrace();
//        }
//
//        // Extracting the average round trip time from the inputLine string
//        if(inputLine!=null && inputLine.contains("=") && inputLine.contains("/")) {
//            String afterEqual = inputLine.substring(inputLine.indexOf("="), inputLine.length()).trim();
//            String afterFirstSlash = afterEqual.substring(afterEqual.indexOf('/') + 1, afterEqual.length()).trim();
//            String strAvgRtt = afterFirstSlash.substring(0, afterFirstSlash.indexOf('/'));
//            avgRtt = Double.valueOf(strAvgRtt);
//        }
        //// Comment out for March Exercise - LTE Case

        StatusReporter.addRTT2Device(ipAddress, avgRtt);
    }
}
