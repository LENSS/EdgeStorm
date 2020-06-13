package com.lenss.cmy;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;

import org.apache.log4j.Logger;


/**
 * Created by cmy on 9/17/19.
 */

public class OnClearFromRecentService extends Service {

    private final String TAG="OnClearFromRecentService";
    Logger logger = Logger.getLogger(TAG);

    @Override
    public IBinder onBind(Intent intent) {
        logger.info("*****************onBind****************************");
        return null;
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        logger.info("*****************onStartCommand****************************");
        return START_NOT_STICKY;
    }

    @Override
    public void onDestroy() {
        logger.info("***************onDestroy******************************");
        super.onDestroy();
    }

    @Override
    public void onTaskRemoved(Intent rootIntent) {
        logger.info("*******************onTaskRemoved**************************");
        stopSelf();
    }
}
