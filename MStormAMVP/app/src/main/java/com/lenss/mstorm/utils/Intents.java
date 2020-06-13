package com.lenss.mstorm.utils;


import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.content.pm.ResolveInfo;

import java.util.List;

public class Intents {
	public static final String ACTION_START_COMPUTING_NODE = "com.lenss.mstorm.START_COMPUTING";
	public static final String ACTION_STOP_COMPUTING_NODE = "com.lenss.mstorm.STOP_COMPUTING";
    public static final String ACTION_START_SUPERVISOR = "com.lenss.mstorm.START_SUPERVISOR";
    public static final String ACTION_STOP_SUPERVISOR = "com.lenss.mstorm.STOP_SUPERVISOR";

    /** From Android L Starting a Service requiring a explicit intent*/
    public static Intent createExplicitFromImplicitIntent(Context context, Intent implicitIntent) {
        // Retrieve all services that can match the given intent
        PackageManager pm = context.getPackageManager();
        List<ResolveInfo> resolveInfo = pm.queryIntentServices(implicitIntent, 0);

        // Make sure only one match was found
        if (resolveInfo == null || resolveInfo.size() != 1) {
            return null;
        }

        // Get component info and create ComponentName
        ResolveInfo serviceInfo = resolveInfo.get(0);
        String packageName = serviceInfo.serviceInfo.packageName;
        String className = serviceInfo.serviceInfo.name;
        ComponentName component = new ComponentName(packageName, className);

        // Create a new intent. Use the old one for extras and such reuse
        Intent explicitIntent = new Intent(implicitIntent);

        // Set the component to be explicit
        explicitIntent.setComponent(component);

        return explicitIntent;
    }
}
