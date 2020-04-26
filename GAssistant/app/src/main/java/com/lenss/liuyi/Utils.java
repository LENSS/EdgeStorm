package com.lenss.liuyi;

import android.content.Context;
import android.os.Environment;
import androidx.annotation.NonNull;
import androidx.annotation.RawRes;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by cmy on 6/23/16.
 */
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
            double k=1.0;
            int innerLoopTimes = (int) (550000 * (1+randomVarPercent));
            for(int j=0; j<innerLoopTimes;j++)
            {
                k=k*1.0;
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

    public static void initLogger(String loggerFilePath) throws IOException {
        //First let the logger show the messages to System.out
        Logger logger = Logger.getRootLogger();
        logger.addAppender(new ConsoleAppender(new PatternLayout("[%-5p] %d (%c{1}): %m%n"), "System.out"));
        logger.info("Trying to initialize logger at "+loggerFilePath);
        PatternLayout layout = new PatternLayout("[%-5p] %d (%c{1}): %m%n");
        RollingFileAppender appender = new RollingFileAppender(layout, loggerFilePath);
        appender.setName("myFirstLog");
        appender.setMaxFileSize("4GB");
        appender.activateOptions();
        logger.addAppender(appender);
        logger.info("\n\n======================== Starting new Process ============================");
        logger.info("Logger initialized at "+loggerFilePath);
    }

    @NonNull
    public static final void copyFileFromRawToOthers(@NonNull final Context context, @RawRes int id, @NonNull final String targetPath) {
        InputStream in = context.getResources().openRawResource(id);
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(targetPath);
            byte[] buff = new byte[1024];
            int read = 0;
            while ((read = in.read(buff)) > 0) {
                out.write(buff, 0, read);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void copyFile(String srcPath, String targetPath) {
        InputStream in = null;
        OutputStream out = null;
        try {
            in = new FileInputStream(srcPath);
            out = new FileOutputStream(targetPath);
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
