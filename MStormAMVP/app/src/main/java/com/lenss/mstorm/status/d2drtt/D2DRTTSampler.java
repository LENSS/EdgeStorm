package com.lenss.mstorm.status.d2drtt;

import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import android.os.Message;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cmy on 1/13/17.
 */
public class D2DRTTSampler {
    private AtomicInteger mSamplingCounter;
    private SamplingHandler mHandler;
    private HandlerThread mThread;
    private ArrayList<String> mAddresses;

    private static class D2DRTTSamplerHolder {
        public static final D2DRTTSampler instance = new D2DRTTSampler();
    }

    public static D2DRTTSampler getInstance() {
        return D2DRTTSamplerHolder.instance;
    }

    public D2DRTTSampler(){
        mSamplingCounter = new AtomicInteger();
        mThread = new HandlerThread("D2DRTTSamplerThread");
        mThread.start();
        mHandler = new SamplingHandler(mThread.getLooper());
    }

    public void startSampling() {
        if (mSamplingCounter.getAndIncrement() == 0) {
            mHandler.startSamplingThread();
        }
    }

    public void stopSampling() {
        if (mSamplingCounter.decrementAndGet() == 0) {
            mHandler.stopSamplingThread();
        }
    }

    public boolean isSampling() {
        return (mSamplingCounter.get() != 0);
    }

    public void setAddresses(ArrayList<String> addresses){
        mAddresses = addresses;
    }

    public void pingToMultipleAddress(){
        for (String addr: mAddresses){
            new Thread(new D2DRTTAverage(addr)).start();
        }
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
                    pingToMultipleAddress();
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
