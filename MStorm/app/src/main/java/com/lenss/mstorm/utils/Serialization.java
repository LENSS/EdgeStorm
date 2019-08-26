package com.lenss.mstorm.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Serialization {
    private static Gson mGSON=  new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

    public static String Serialize(Object obj) {
        return mGSON.toJson(obj);
    }

    public static Object Deserialize(String json,Class cls) {
        return mGSON.fromJson(json,cls);
    }
}
