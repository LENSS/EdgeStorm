package com.lenss.mstorm.status.bandwidth;

import android.net.TrafficStats;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import android.os.Message;
import android.os.SystemClock;

import java.util.concurrent.atomic.AtomicInteger;

public class DeviceBandwidthSampler {
    private final ConnectionClassManager mConnectionClassManager;
    private AtomicInteger mSamplingCounter;
    private SamplingHandler mHandler;
    private HandlerThread mThread;
    private long mLastTimeReading;
    private int uID;
    private static long sPreviousBytesDown = -1;
    private static long sPreviousBytesUp = -1;

    // Singleton.
    private static class DeviceBandwidthSamplerHolder {
        public static final DeviceBandwidthSampler instance = new DeviceBandwidthSampler(ConnectionClassManager.getInstance());
    }

    public static DeviceBandwidthSampler getInstance() {
        return DeviceBandwidthSamplerHolder.instance;
    }

    private DeviceBandwidthSampler(ConnectionClassManager connectionClassManager) {
        mConnectionClassManager = connectionClassManager;
        mSamplingCounter = new AtomicInteger();
        mThread = new HandlerThread("DeviceBandwidthSamplerThread");
        mThread.start();
        mHandler = new SamplingHandler(mThread.getLooper());
    }

    public void setUID(int uid){
        uID = uid;
    }

    public void startSampling() {
        if (mSamplingCounter.getAndIncrement() == 0) {
            mHandler.startSamplingThread();
            mLastTimeReading = SystemClock.elapsedRealtimeNanos();
        }
    }

    public void stopSampling() {
        if (mSamplingCounter.decrementAndGet() == 0) {
            mHandler.stopSamplingThread();
            addFinalSample();
        }
    }

    protected void addSample() {
        long newBytesDown = TrafficStats.getUidRxBytes(uID);
        long newBytesUp = TrafficStats.getUidTxBytes(uID);
        long byteDiffDown = newBytesDown - sPreviousBytesDown;
        long byteDiffUp = newBytesUp - sPreviousBytesUp;

        if (sPreviousBytesDown >= 0) {
            synchronized (this) {
                long curTimeReading = SystemClock.elapsedRealtimeNanos();
                mConnectionClassManager.addDownBandwidth(byteDiffDown, curTimeReading - mLastTimeReading);
                mConnectionClassManager.addUpBandwidth(byteDiffUp, curTimeReading - mLastTimeReading);

                mLastTimeReading = curTimeReading;
            }
        }
        sPreviousBytesDown = newBytesDown;
        sPreviousBytesUp = newBytesUp;
    }

    /**
     * Resets previously read byte count after recording a sample, so that
     * we don't count bytes downloaded in between sampling sessions.
     */
    protected void addFinalSample() {
        addSample();
        sPreviousBytesDown = -1;
        sPreviousBytesUp = -1;
    }

    public boolean isSampling() {
        return (mSamplingCounter.get() != 0);
    }

    private class SamplingHandler extends Handler {
        static final long SAMPLE_TIME = 5000;     // sampling every 5s
        static private final int MSG_START = 1;

        public SamplingHandler(Looper looper) {
            super(looper);
        }

        @Override
        public void handleMessage(Message msg) {
            switch (msg.what) {
                case MSG_START:
                    addSample();
                    sendEmptyMessageDelayed(MSG_START, SAMPLE_TIME);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown what=" + msg.what);
            }
        }

        public void startSamplingThread() {
            sendEmptyMessage(SamplingHandler.MSG_START);
        }

        public void stopSamplingThread() {
            removeMessages(SamplingHandler.MSG_START);
        }
    }

}
