package utils;

import java.net.InetSocketAddress;
import java.util.Random;

public class Helper {
	private static int portNum = 10000;
    public  static InetSocketAddress getInetSocketAddress(String addr)
    {
        String lhostname=addr.substring(0, addr.indexOf(':'));
        int lport= Integer.parseInt(addr.substring(addr.indexOf(':')+1, addr.length() ));
        InetSocketAddress local=new InetSocketAddress(lhostname,lport);
        return local;
    }
    
    public static int randInt(int min, int max) {

        // NOTE: Usually this should be a field rather than a method
        // variable so that it is not re-seeded every call.
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }
    
    public static int getNextPort(){
    	return portNum++;
    }
    
    public static double  minimium( double[] array){
    	double min = array[0];
    	for (int i = 1; i< array.length; i++){
    		if(array[i] < min)
    			min = array[i];
    	}
    	return min;
    }
    
    public static int approCeil(double total, double unit){
    	double ratio = total/unit;
    	int strictCeil = (int) Math.ceil(ratio);
    	if((ratio-strictCeil+1)<0.05 && strictCeil!=1)
    		return (strictCeil-1);
    	else
    		return strictCeil;  	
    }
    
    public static int approFloor(double total, double unit){
    	double ratio = total/unit;
    	int strictCeil = (int) Math.floor(ratio);
    	if((strictCeil+1-ratio)<0.05)
    		return strictCeil+1;
    	else
    		return strictCeil;
    	
    }
}
