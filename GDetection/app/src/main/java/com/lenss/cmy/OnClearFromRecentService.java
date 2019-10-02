package com.lenss.cmy;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;


/**
 * Created by cmy on 9/17/19.
 */

public class OnClearFromRecentService extends Service {

    @Override
    public IBinder onBind(Intent intent) {
        System.out.println("*****************onBind****************************");
        return null;
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        System.out.println("*****************onStartCommand****************************");
        return START_NOT_STICKY;
    }

    @Override
    public void onDestroy() {
        System.out.println("***************onDestroy******************************");
        super.onDestroy();
    }

    @Override
    public void onTaskRemoved(Intent rootIntent) {
        System.out.println("*******************onTaskRemoved**************************");
        stopSelf();
    }
}
