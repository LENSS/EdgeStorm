package com.lenss.mstorm.utils;

import com.lenss.mstorm.status.StatusReporter;

import java.util.concurrent.CopyOnWriteArrayList;

public class StatisticsCalculator {
    public static double LARGE_VALUE = 1000000;
    public static double SMALL_VALUE = 0.001;

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

    public static double getAvgTime(CopyOnWriteArrayList<Long> timeRecord, int type){
        double averageTime;

        int size = timeRecord.size();

        if(type == StatusReporter.UPSTREAM){
            if(size==0) {
                averageTime = LARGE_VALUE;   // ms
            } else {
                double totalTime = 0.0;
                for (int i = 0; i < size; i++){
                    totalTime += timeRecord.get(i);
                }
                averageTime = totalTime/size/1000000.0;  // ms
            }
        } else {
            if(size==0) {
                averageTime = LARGE_VALUE;   // ms
            } else {
                double totalTime = 0.0;
                for (int i = 0; i < size; i++){
                    totalTime += timeRecord.get(i);
                }
                averageTime = totalTime/size/1000000.0;  // ms
            }
        }

        return averageTime;
    }

    public static double getThroughput(CopyOnWriteArrayList<Long> entryTimeRecord, int type) {
        double throughput;

        int size = entryTimeRecord.size();

        if(type == StatusReporter.UPSTREAM) {
            if(size == 0){
                throughput = SMALL_VALUE; // tuple/s
            } else {
                throughput = 1.0 * size / StatusReporter.REPORT_PERIOD_TO_UPSTREAM * 1000; // tuple/s
            }
        } else {
            if(size == 0){
                throughput = SMALL_VALUE; // tuple/s
            } else {
                throughput = 1.0 * size / StatusReporter.REPORT_PERIOD_TO_NIMBUS * 1000; // tuple/s
            }
        }

        return throughput;
    }
}
