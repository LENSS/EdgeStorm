package com.lenss.cmy.grecognition;

import static org.junit.Assume.assumeNoException;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import javax.imageio.ImageIO;

import org.apache.log4j.Logger;
import org.datavec.image.loader.NativeImageLoader;
import org.deeplearning4j.nn.graph.ComputationGraph;
import org.deeplearning4j.nn.transferlearning.TransferLearningHelper;
import org.deeplearning4j.zoo.PretrainedType;
import org.deeplearning4j.zoo.ZooModel;
import org.deeplearning4j.zoo.model.VGG16;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization;
import org.nd4j.linalg.dataset.api.preprocessor.VGG16ImagePreProcessor;
import org.nd4j.linalg.factory.Nd4j;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Processor;
import Models.PersonModel;

public class MyFaceRecognizer extends Processor {
	Logger logger = Logger.getLogger("MyFaceRecognizer");
	
	private TransferLearningHelper transferLearningHelper ;
	private NativeImageLoader nativeImageLoader;
	private DataNormalization scaler;
	private List<PersonModel> trainList;
	
	public List<PersonModel> readTrainedFacesFromDisk(){
	    List<PersonModel> faceList = null;
	    File modelFile = new File("faceModel");
	    if(modelFile.exists()) {
	       try{
	         FileInputStream fis = new FileInputStream("faceModel");
	         ObjectInputStream ois = new ObjectInputStream(fis);
	         faceList = (List<PersonModel>) ois.readObject();
	         ois.close();
	       } catch(IOException | ClassNotFoundException e){
	         e.printStackTrace();
	       }
	    } else {
	       faceList = new ArrayList<>();
	    }
	    return faceList;
	}
	
	@Override
	public void prepare() {
		logger.info("Loading DL4J and FaceRecognizer");    
	    ZooModel objZooModel = new VGG16();
	    logger.info("Create VGG16 ......");
	    ComputationGraph objComputationGraph = null;
	    try {
	    	objComputationGraph = (ComputationGraph)objZooModel.initPretrained(PretrainedType.VGGFACE);	
	    	logger.info("Intialize with pretrained model ......");
	    } catch (IOException e) {
	    	logger.info("Can NOT intialize with pretrained model ......");
			e.printStackTrace();
	    }
	    transferLearningHelper = new TransferLearningHelper(objComputationGraph,"pool4");
		logger.info("Create a transfer learning helper ......");
		nativeImageLoader = new NativeImageLoader(224, 224, 3);
		logger.info("Image loader created ......");
		scaler = new VGG16ImagePreProcessor();
		logger.info("VGG16 Preprocessor created ......");
		trainList = readTrainedFacesFromDisk(); 
	    logger.info("Successfully loaded DL4J and FaceRecognizer");
	}
    
	@Override
	public void execute() {
		int taskID = getTaskID();
        while (!Thread.currentThread().isInterrupted()) {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(getTaskID());
            if(pktRecv != null){
            	long enterTime = System.nanoTime();
                byte[] frame = pktRecv.complexContent;
	            logger.info("FACE RECOGNIZER RECEIVES A FRAME, "+ getTaskID());
	            // read byte frame into image and recognize
	            try {
	            	// get feature of 
	            	BufferedImage img = ImageIO.read(new ByteArrayInputStream(frame));
					INDArray imageMatrix = nativeImageLoader.asMatrix(img);
					scaler.transform(imageMatrix);
					DataSet objDataSet = new DataSet(imageMatrix, Nd4j.create(new float[]{0,0}));
				    DataSet objFeaturized = transferLearningHelper.featurize(objDataSet);
				    INDArray featuresArray = objFeaturized.getFeatures();				 
				    int reshapeDimension=1;
				    for (int dimension : featuresArray.shape()) {
				    	reshapeDimension *= dimension;
				    }
				    featuresArray = featuresArray.reshape(1,reshapeDimension);
				    
				    double minimalDistance = Double.MAX_VALUE;
				    String recognizedFace = "";
				    for(PersonModel personModel : trainList)
				    {
				      INDArray personArray = Nd4j.create(personModel.get_faceFeatureArray());
				      double distance = featuresArray.distance2(personArray);
				      if (distance<minimalDistance){
				        minimalDistance = distance;
				        recognizedFace = personModel.get_personName();
				      }
				    }
				    InternodePacket pktSend = new InternodePacket();
				    pktSend.ID = pktRecv.ID;
                    pktSend.type = InternodePacket.TYPE_DATA;
                    pktSend.fromTask = getTaskID();
                    pktSend.simpleContent.put("name",recognizedFace);
                    pktSend.complexContent = frame;
                    pktSend.traceTask = pktRecv.traceTask;
                    pktSend.traceTask.add("MFR_"+taskID);
                    pktSend.traceTaskEnterTime = pktRecv.traceTaskEnterTime;
                    pktSend.traceTaskEnterTime.put("MFR_"+taskID, enterTime);
                    pktSend.traceTaskExitTime = pktRecv.traceTaskExitTime;
                    long exitTime = System.nanoTime();
                    pktSend.traceTaskExitTime.put("MFR_"+taskID, exitTime);
                    String component = MyFaceSaver.class.getName();
                    try {
                        MessageQueues.emit(pktSend, getTaskID(), component);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
				} catch (IOException e) {
					e.printStackTrace();
				}
            }
        }
	}
}
