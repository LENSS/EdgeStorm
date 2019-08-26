package com.lenss.mstorm.utils;

import com.lenss.mstorm.status.StatusReporter;

import java.util.concurrent.CopyOnWriteArrayList;

public class StatisticsCalculator {
    public static double getAvg(double[] array){
        double sum = 0;
        for (int i=0;i<array.length;i++){
            sum += array[i];
        }
        return sum/array.length;
    }

    public static double getStdDev(double[] array){
        double mean = getAvg(array);
        double squareSum = 0;
        for (int i=0;i<array.length;i++){
            squareSum += Math.pow((array[i] - mean), 2);
        }
        double stdDev = Math.sqrt(squareSum / array.length);
        return stdDev;
    }


    public static double getAvgTime(CopyOnWriteArrayList<Long> timeRecord){
        double averageTime;

        int size = timeRecord.size();
        if(size==0) {
            averageTime = StatusReporter.END2ENDDELAY_THRESHOLD;       // ms
        } else {
            double totalTime = 0.0;
            for (int i=0;i<size;i++){
                totalTime += timeRecord.get(i);
            }
            averageTime = totalTime/size/1000000.0;      // ms
        }
        return averageTime;
    }

    public static double getThroughput(CopyOnWriteArrayList<Long> entryTimeRecord, int type) {
        double throughput;

        int size = entryTimeRecord.size();

        if(type == StatusReporter.THROUGHPUT_UPSTREAM) {
            if(size <=1){
                throughput = 1.0 / StatusReporter.END2ENDDELAY_THRESHOLD * 1000;
            } else {
                throughput = 1.0 * size / StatusReporter.REPORT_PERIOD_TO_UPSTREAM * 1000;
            }
        } else {
            if(size <=1){
                throughput = 1.0 / StatusReporter.END2ENDDELAY_THRESHOLD * 1000;
            } else {
                throughput = 1.0 * size / StatusReporter.REPORT_PERIOD_TO_NIMBUS * 1000;
            }
        }
        return throughput;
    }

    public static double getInput(CopyOnWriteArrayList<Long> entryTimeRecord){
        int size = entryTimeRecord.size();
        double input = 1.0 * size /(entryTimeRecord.get(size-1)-entryTimeRecord.get(0)) * 1000000000.0;
        return input;
    }
}
