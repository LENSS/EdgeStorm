package com.tzutalin.dlib;

import android.os.Environment;

import java.io.File;

public final class Constants {
    private Constants() {
        // Constants should be prive
    }

    public static String getDLibDirectoryPath() {
        File sdcard = Environment.getExternalStorageDirectory();
        String targetPath = sdcard.getAbsolutePath() + File.separator + "distressnet" + File.separator + "MStorm" + File.separator + "Dlib";
        return targetPath;
    }

    public static String getDLibImageDirectoryPath() {
        String targetPath = getDLibDirectoryPath()+ File.separator + "images";
        return targetPath;
    }

    public static String getFaceShapeModelPath() {
        String targetPath = getDLibDirectoryPath() + File.separator + "shape_predictor_5_face_landmarks.dat";
        return targetPath;
    }

    public static String getFaceDescriptorModelPath() {
        String targetPath = getDLibDirectoryPath() + File.separator + "dlib_face_recognition_resnet_model_v1.dat";
        return targetPath;
    }
}
