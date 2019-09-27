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
	    System.out.println("Loading DL4J and FaceRecognizer");    
	    ZooModel objZooModel = new VGG16();
	    System.out.println("Test 1 ......");
	    ComputationGraph objComputationGraph = null;
	    try {
	    	objComputationGraph = (ComputationGraph)objZooModel.initPretrained(PretrainedType.VGGFACE);	
	    	System.out.println("Test 2 ......");
	    } catch (IOException e) {
	    	System.out.println("Test 3 ......");
			e.printStackTrace();
	    }
	    transferLearningHelper = new TransferLearningHelper(objComputationGraph,"pool4");
		System.out.println("Test 4 ......");
		nativeImageLoader = new NativeImageLoader(224, 224, 3);
		System.out.println("Test 5 ......");
		scaler = new VGG16ImagePreProcessor();
		System.out.println("Test 6 ......");
		trainList = readTrainedFacesFromDisk(); 
	    System.out.println("Loaded DL4J and FaceRecognizer");
	}
    
	@Override
	public void execute() {
        while (!Thread.currentThread().isInterrupted()) {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(getTaskID());
            if(pktRecv != null){
                byte[] frame = pktRecv.complexContent;
	            System.out.println("FACE RECOGNIZER RECEIVES A FRAME, "+ getTaskID());
	            // read byte frame into image and recognize
	            try {
	            	// get feature of 
	            	BufferedImage img = ImageIO.read(new ByteArrayInputStream(frame));
					INDArray imageMatrix = nativeImageLoader.asMatrix(img);
					scaler.transform(imageMatrix);
					DataSet objDataSet = new DataSet(imageMatrix, Nd4j.create(new float[]{0,0}));
					System.out.println(transferLearningHelper);
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
				    System.out.println(recognizedFace);
				    InternodePacket pktSend = new InternodePacket();
                    pktSend.type = InternodePacket.TYPE_DATA;
                    pktSend.fromTask = getTaskID();
                    pktSend.simpleContent.put("name",recognizedFace);
                    pktSend.complexContent = frame;
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
