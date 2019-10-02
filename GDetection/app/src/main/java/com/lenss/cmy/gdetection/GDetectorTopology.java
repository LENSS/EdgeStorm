package com.lenss.cmy.gdetection;

import com.lenss.cmy.GDetectionActivity;
import com.lenss.mstorm.topology.Topology;
import java.util.ArrayList;

/**
 * Created by cmy on 8/14/19.
 */

public class GDetectorTopology {
    public static Topology createTopology(){
        Topology mTopology=new Topology(3);

        MyPictureDistributor pd = new MyPictureDistributor();
        mTopology.setDistributor(pd, GDetectionActivity.myDistributorParallel, GDetectionActivity.myDistributorScheduleReq);

        MyFaceDetector fd=new MyFaceDetector();
        mTopology.setProcessor(fd, GDetectionActivity.faceDetectorParallel, GDetectionActivity.faceDetectorGroupingMethod, GDetectionActivity.faceDetectorScheduleReq);

        MyFaceSaver fs = new MyFaceSaver();
        mTopology.setProcessor(fs, GDetectionActivity.faceSaverParallel, GDetectionActivity.faceSaverGroupingMethod, GDetectionActivity.faceSaverScheduleReq);

        // set the relationship between each component in the topology
        ArrayList<Object> pdDownStreamComponents = new ArrayList<Object>();
        pdDownStreamComponents.add(fd);
        mTopology.setDownStreamComponents(pd,pdDownStreamComponents);

        ArrayList<Object> fdDownStreamComponents = new ArrayList<Object>();
        fdDownStreamComponents.add(fs);
        mTopology.setDownStreamComponents(fd,fdDownStreamComponents);

        return mTopology;
    }
}
