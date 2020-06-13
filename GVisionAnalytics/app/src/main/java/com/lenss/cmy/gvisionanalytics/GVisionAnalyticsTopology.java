package com.lenss.cmy.gvisionanalytics;

import com.lenss.cmy.GVisionAnalyticsActivity;
import com.lenss.mstorm.topology.Topology;
import java.util.ArrayList;

/**
 * Created by cmy on 8/14/19.
 */

public class GVisionAnalyticsTopology {
    public static Topology createTopology(){
        Topology mTopology=new Topology(5);  // remember to configure correct comp num !!!

        MyPictureDistributor pd = new MyPictureDistributor();
        mTopology.setDistributor(pd, GVisionAnalyticsActivity.myDistributorParallel, GVisionAnalyticsActivity.myDistributorScheduleReq);

//        MyFaceDetector fd=new MyFaceDetector();
//        mTopology.setProcessor(fd, GVisionAnalyticsActivity.faceDetectorParallel, GVisionAnalyticsActivity.faceDetectorGroupingMethod, GVisionAnalyticsActivity.faceDetectorScheduleReq);

        MyAgeAnalyzer aa = new MyAgeAnalyzer();
        mTopology.setProcessor(aa, GVisionAnalyticsActivity.ageAnalyzerParallel, GVisionAnalyticsActivity.ageAnalyzerGroupingMethod, GVisionAnalyticsActivity.ageAnalyzerScheduleReq);

        MyGenderAnalyzer ga = new MyGenderAnalyzer();
        mTopology.setProcessor(ga, GVisionAnalyticsActivity.genderAnalyzerParallel, GVisionAnalyticsActivity.genderAnalyzerGroupingMethod, GVisionAnalyticsActivity.genderAnalyzerScheduleReq);

        MyEmotionAnalyzer ea = new MyEmotionAnalyzer();
        mTopology.setProcessor(ea, GVisionAnalyticsActivity.emotionAnalyzerParallel, GVisionAnalyticsActivity.emotionAnalyzerGroupingMethod, GVisionAnalyticsActivity.emotionAnalyzerScheduleReq);

//        MyFaceCommonFeature fcf = new MyFaceCommonFeature();
//        mTopology.setProcessor(fcf, GVisionAnalyticsActivity.ageAnalyzerParallel, GVisionAnalyticsActivity.ageAnalyzerGroupingMethod, GVisionAnalyticsActivity.ageAnalyzerScheduleReq);
//
//        MyGenderAnalyzerFromFeature gaff = new MyGenderAnalyzerFromFeature();
//        mTopology.setProcessor(gaff, GVisionAnalyticsActivity.genderAnalyzerParallel, GVisionAnalyticsActivity.genderAnalyzerGroupingMethod, GVisionAnalyticsActivity.genderAnalyzerScheduleReq);
//
//        MyEmotionAnalyzerFromFeature eaff = new MyEmotionAnalyzerFromFeature();
//        mTopology.setProcessor(eaff, GVisionAnalyticsActivity.emotionAnalyzerParallel, GVisionAnalyticsActivity.emotionAnalyzerGroupingMethod, GVisionAnalyticsActivity.emotionAnalyzerScheduleReq);
//
//        MyAgeAnalyzerFromFeature aaff = new MyAgeAnalyzerFromFeature();
//        mTopology.setProcessor(aaff, GVisionAnalyticsActivity.ageAnalyzerParallel, GVisionAnalyticsActivity.ageAnalyzerGroupingMethod, GVisionAnalyticsActivity.ageAnalyzerScheduleReq);

        MyFaceSaver fs = new MyFaceSaver();
        mTopology.setProcessor(fs, GVisionAnalyticsActivity.faceSaverParallel, GVisionAnalyticsActivity.faceSaverGroupingMethod, GVisionAnalyticsActivity.faceSaverScheduleReq);

        // set the relationship between each component in the topology
//        ArrayList<Object> pdDownStreamComponents = new ArrayList<Object>();
//        pdDownStreamComponents.add(fcf);       // change back to fd !!!!!!
//        mTopology.setDownStreamComponents(pd,pdDownStreamComponents);

        // model splitting case
//        ArrayList<Object> fdDownStreamComponents = new ArrayList<Object>();
//        fdDownStreamComponents.add(fcf);
//        mTopology.setDownStreamComponents(fd,fdDownStreamComponents);

//        ArrayList<Object> fcfDownStreamComponents = new ArrayList<Object>();
//        fcfDownStreamComponents.add(gaff);
//        fcfDownStreamComponents.add(eaff);
//        fcfDownStreamComponents.add(aaff);
//
//        mTopology.setDownStreamComponents(fcf,fcfDownStreamComponents);
//
//        ArrayList<Object> gaffDownStreamComponents = new ArrayList<Object>();
//        gaffDownStreamComponents.add(fs);
//        mTopology.setDownStreamComponents(gaff, gaffDownStreamComponents);
//
//        ArrayList<Object> eaffDownStreamComponents = new ArrayList<Object>();
//        eaffDownStreamComponents.add(fs);
//        mTopology.setDownStreamComponents(eaff, eaffDownStreamComponents);
//
//        ArrayList<Object> aaffDownStreamComponents = new ArrayList<Object>();
//        aaffDownStreamComponents.add(fs);
//        mTopology.setDownStreamComponents(aaff, aaffDownStreamComponents);

//        // no model splitting case
        ArrayList<Object> pdDownStreamComponents = new ArrayList<Object>();
        pdDownStreamComponents.add(aa);
        pdDownStreamComponents.add(ga);
        pdDownStreamComponents.add(ea);
        mTopology.setDownStreamComponents(pd,pdDownStreamComponents);

        ArrayList<Object> aaDownStreamComponents = new ArrayList<Object>();
        aaDownStreamComponents.add(fs);
        mTopology.setDownStreamComponents(aa,aaDownStreamComponents);

        ArrayList<Object> gaDownStreamComponents = new ArrayList<Object>();
        gaDownStreamComponents.add(fs);
        mTopology.setDownStreamComponents(ga,gaDownStreamComponents);

        ArrayList<Object> eaDownStreamComponents = new ArrayList<Object>();
        eaDownStreamComponents.add(fs);
        mTopology.setDownStreamComponents(ea,eaDownStreamComponents);

        return mTopology;
    }
}
