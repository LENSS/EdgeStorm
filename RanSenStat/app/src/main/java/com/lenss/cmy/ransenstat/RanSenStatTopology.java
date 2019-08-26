package com.lenss.cmy.ransenstat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.utils.Serialization;

import java.lang.reflect.Type;
import java.util.ArrayList;

/**
 * Created by cmy on 6/29/16.
 */
public class RanSenStatTopology {

    public static Topology createTopology(){
        Topology mTopology=new Topology(4);

        SentenceDistributor senDist=new SentenceDistributor();
        senDist.setWorkload(RanSenStatActivity.senDistributorWorkload);
        mTopology.setDistributor(senDist, RanSenStatActivity.senDistributorParallel,
                RanSenStatActivity.senDistributorScheduleReq);

        TopicTagger topTag=new TopicTagger();
        topTag.setWorkload(RanSenStatActivity.topicTaggerWorkload);
        mTopology.setProcessor(topTag, RanSenStatActivity.topicTaggerParallel,
                RanSenStatActivity.topicTaggerGroupingMethod, RanSenStatActivity.topicTaggerScheduleReq);

        KeyWordStatistics keyWordStat = new KeyWordStatistics();
        keyWordStat.setWorkload(RanSenStatActivity.keywordStatWorkload);
        mTopology.setProcessor(keyWordStat, RanSenStatActivity.keywordStatParallel,
                RanSenStatActivity.keywordStatGroupingMethod, RanSenStatActivity.keywordStatScheduleReq);

        SentenceSink senSink = new SentenceSink();
        senSink.setWorkLoad(RanSenStatActivity.senSinkWorkload);
        mTopology.setProcessor(senSink, RanSenStatActivity.senSinkParallel,
                RanSenStatActivity.senSinkGroupingMethod, RanSenStatActivity.senSinkScheduleReq);

        // set the relationship between each component in the topology
        ArrayList<Object> downStreamComponentsOfSenDist = new ArrayList<Object>();
        downStreamComponentsOfSenDist.add(topTag);
        mTopology.setDownStreamComponents(senDist, downStreamComponentsOfSenDist);

        ArrayList<Object> downStreamComponentsOfTopTag = new ArrayList<Object>();
        downStreamComponentsOfTopTag.add(keyWordStat);
        mTopology.setDownStreamComponents(topTag, downStreamComponentsOfTopTag);

        ArrayList<Object> downStreamComponentsOfKeyWordStat = new ArrayList<Object>();
        downStreamComponentsOfKeyWordStat.add(senSink);
        mTopology.setDownStreamComponents(keyWordStat, downStreamComponentsOfKeyWordStat);

        return mTopology;
    }
}
