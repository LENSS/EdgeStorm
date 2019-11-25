package com.lenss.mstorm.status.d2drtt;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.status.StatusReporter;
import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.StatisticsCalculator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by cmy on 1/13/17.
 */
public class D2DRTTAverage implements Runnable {

    private final int NUMBER_OF_PACKTETS = 20;
    private final double interval = 0.2;  // 200ms
    private final int size = 1500;  // 1.5k
    private String address;
    private String ipAddress;

    public D2DRTTAverage(String addr){
        address = addr;
        ipAddress = GNSServiceHelper.getIPInUseByGUID(address);
    }

    /**
        Returns the latency to a given server in mili-seconds by issuing a ping command.
        system will issue NUMBER_OF_PACKTETS ICMP Echo Request packet each having size of 56 bytes
        every second, and returns the avg latency of them.
        Returns 0 when there is no connection
    */
    @Override
    public void run(){
        double avgRtt;
        double linkQuality;

        // Comment out for March Exercise - LTE Case
        String pingCommand = "/system/bin/ping -c " + NUMBER_OF_PACKTETS + " -i "+ interval + " -s "+ size + " " + ipAddress;
        String inputLine = "";
        String rttLine = "";
        String linkQualityLine = "";

        try {
            // execute the command on the environment interface
            Process process = Runtime.getRuntime().exec(pingCommand);
            // gets the input stream to get the output of the executed command
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));

            inputLine = bufferedReader.readLine();

            while ((inputLine != null)) {
                if (inputLine.length() > 0 && inputLine.contains("avg")) {
                    rttLine = inputLine;
                }
                if (inputLine.length() > 0 && inputLine.contains("received")) {
                    linkQualityLine = inputLine;
                }
                inputLine = bufferedReader.readLine();
            }
        } catch (IOException e){
            e.printStackTrace();
        }

        // Extracting the average round trip time from the inputLine string
        if(rttLine!="" && rttLine.contains("=") && rttLine.contains("/")) {
            String afterEqual = rttLine.substring(rttLine.indexOf("=")).trim();
            String afterFirstSlash = afterEqual.substring(afterEqual.indexOf('/') + 1).trim();
            String strAvgRtt = afterFirstSlash.substring(0, afterFirstSlash.indexOf('/'));
            avgRtt = Double.valueOf(strAvgRtt);
        } else {
            avgRtt = NUMBER_OF_PACKTETS * interval * 1000.0;    //ms
        }

        if(linkQualityLine!="" && linkQualityLine.contains("received")){
            String[] result = linkQualityLine.split("\\s+");
            int transmitted = Integer.parseInt(result[0]);
            int received = Integer.parseInt(result[3]);
            linkQuality = 1.0 * received / transmitted;
        } else {
            linkQuality = StatisticsCalculator.SMALL_VALUE;
        }
        // Comment out for March Exercise - LTE Case
        StatusReporter.addRTT2Device(address, avgRtt);
        StatusReporter.addLinkQuality2Device(address, linkQuality);
    }
}
