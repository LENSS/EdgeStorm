package com.lenss.mstorm.status;

public class StatusReporter {
	// Two report type
    public static final int NIMBUS = 1;
    public static final int UPSTREAM = 2;

    // Report period to Nimbus
    public static final int REPORT_PERIOD_TO_NIMBUS = 30000;   // 30s
    // Report period to upstream tasks
    public static final int REPORT_PERIOD_TO_UPSTREAM = 10000;  //10s
}
