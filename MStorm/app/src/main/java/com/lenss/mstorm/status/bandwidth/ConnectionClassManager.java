package com.lenss.mstorm.status.bandwidth;

public class ConnectionClassManager {

    private static final int BYTES_TO_BITS = 8;

    /**
     * The lower bound for measured bandwidth in bits/ms. Readings
     * lower than this are treated as effectively zero (therefore ignored).
     */
    static final long BANDWIDTH_LOWER_BOUND = 1;

    /**
     * The factor used to calculate the current bandwidth
     * depending upon the previous calculated value for bandwidth.
     * The smaller this value is, the less responsive to new samples the moving average becomes.
     */
    private static final double DEFAULT_DECAY_CONSTANT = 0.05;

    private ExponentialGeometricAverage mDownloadBandwidth = new ExponentialGeometricAverage(DEFAULT_DECAY_CONSTANT);

    private ExponentialGeometricAverage mUploadBandwidth = new ExponentialGeometricAverage(DEFAULT_DECAY_CONSTANT);

    private static class ConnectionClassManagerHolder {
        public static final ConnectionClassManager instance = new ConnectionClassManager();
    }

    public static ConnectionClassManager getInstance() {
        return ConnectionClassManagerHolder.instance;
    }

    private ConnectionClassManager() {}

    public synchronized void addDownBandwidth(long bytes, long timeInMs) {
        //Ignore garbage values.
        double bandwidth = (bytes) * 1.0 / (timeInMs) * BYTES_TO_BITS;

        if (timeInMs == 0 || bandwidth < BANDWIDTH_LOWER_BOUND) {
            return;
        }

        mDownloadBandwidth.addMeasurement(bandwidth);
    }

    public synchronized void addUpBandwidth(long bytes, long timeInMs) {
        //Ignore garbage values.
        double bandwidth = (bytes) * 1.0 / (timeInMs) * BYTES_TO_BITS;
        if (timeInMs == 0 || bandwidth < BANDWIDTH_LOWER_BOUND) {
            return;
        }
        mUploadBandwidth.addMeasurement(bandwidth);
    }

    public synchronized double getDownloadKBitsPerSecond() {
        return mDownloadBandwidth == null ? -1.0 : mDownloadBandwidth.getAverage();
    }

    public synchronized double getUploadKBitsPerSecond() {
        return mUploadBandwidth == null ? -1.0 : mUploadBandwidth.getAverage();
    }

    public void reset() {
        if (mDownloadBandwidth != null) {
            mDownloadBandwidth.reset();
        }
        if (mUploadBandwidth != null) {
            mUploadBandwidth.reset();
        }
    }
}
