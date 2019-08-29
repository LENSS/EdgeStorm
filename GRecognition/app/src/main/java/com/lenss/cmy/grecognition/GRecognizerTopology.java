package com.lenss.cmy.grecognition;

import com.lenss.cmy.GRecognitionActivity;
import com.lenss.mstorm.topology.Topology;
import java.util.ArrayList;

/**
 * Created by cmy on 8/14/19.
 */

public class GRecognizerTopology {
    public static Topology createTopology(){
        Topology mTopology=new Topology(4);

        MyPictureDistributor pd = new MyPictureDistributor();
        mTopology.setDistributor(pd, GRecognitionActivity.myDistributorParallel, GRecognitionActivity.myDistributorScheduleReq);

        MyFaceDetector fd=new MyFaceDetector();
        mTopology.setProcessor(fd, GRecognitionActivity.faceDetectorParallel, GRecognitionActivity.faceDetectorGroupingMethod, GRecognitionActivity.faceDetectorScheduleReq);

        MyFaceRecognizer fr = new MyFaceRecognizer();
        mTopology.setProcessor(fr, GRecognitionActivity.faceRecognizerParallel, GRecognitionActivity.faceRecognizerGroupingMethod, GRecognitionActivity.faceRecognizerScheduleReq);

        MyFaceSaver fs = new MyFaceSaver();
        mTopology.setProcessor(fs, GRecognitionActivity.faceSaverParallel, GRecognitionActivity.faceSaverGroupingMethod, GRecognitionActivity.faceSaverScheduleReq);

        // set the relationship between each component in the topology
        ArrayList<Object> pdDownStreamComponents = new ArrayList<Object>();
        pdDownStreamComponents.add(fd);
        mTopology.setDownStreamComponents(pd,pdDownStreamComponents);

        ArrayList<Object> fdDownStreamComponents = new ArrayList<Object>();
        fdDownStreamComponents.add(fr);
        mTopology.setDownStreamComponents(fd,fdDownStreamComponents);

        ArrayList<Object> frDownStreamComponents = new ArrayList<Object>();
        frDownStreamComponents.add(fs);
        mTopology.setDownStreamComponents(fr,frDownStreamComponents);

        return mTopology;
    }
}
