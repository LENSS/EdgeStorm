package com.lenss.liuyi;
import com.lenss.mstorm.topology.Topology;

import java.util.ArrayList;


public class GAssistantTopology {

    public static Topology createTopology(){
        Topology mTopology = new Topology(3);

        MyVoiceDistributor vd = new MyVoiceDistributor();
        mTopology.setDistributor(vd, GAssistantActivity.voiceReceptorParallel, GAssistantActivity.voiceReceptorScheduleReq);

        MyVoiceConverter vc = new MyVoiceConverter();
        mTopology.setProcessor(vc, GAssistantActivity.voiceConverterParallel, GAssistantActivity.voiceConverterGroupMethod, GAssistantActivity.voiceConverterScheduleReq);

        MyVoiceSaver vs = new MyVoiceSaver();
        mTopology.setProcessor(vs, GAssistantActivity.voiceSaverParallel, GAssistantActivity.voiceSaverGroupMethod, GAssistantActivity.voiceSaverScheduleReq);

        ArrayList<Object> vdDownStreamComponents = new ArrayList<Object>();
        vdDownStreamComponents.add(vc);
        mTopology.setDownStreamComponents(vd, vdDownStreamComponents);

        ArrayList<Object> vcDownStreamComponents = new ArrayList<Object>();
        vcDownStreamComponents.add(vs);
        mTopology.setDownStreamComponents(vc, vcDownStreamComponents);

        return mTopology;
    }
}
