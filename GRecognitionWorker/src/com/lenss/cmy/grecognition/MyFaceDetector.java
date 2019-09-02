package com.lenss.cmy.grecognition;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import org.opencv.core.Rect;
import org.opencv.highgui.HighGui;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.objdetect.CascadeClassifier;

import org.openimaj.image.FImage;
import org.openimaj.image.ImageUtilities;
import org.openimaj.image.processing.face.detection.DetectedFace;
import org.openimaj.image.processing.face.detection.HaarCascadeDetector;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Processor;


public class MyFaceDetector extends Processor {
	private final int openImajFD = 1;
	private HaarCascadeDetector openImajFaceDet = null;
	
	private final int openCVFD = 2;
    private CascadeClassifier openCVFaceDet = null;
    
    private int choosenFD = 0;
	
    private void setupFaceDetectionLib(int faceDectLib) {
    	if(faceDectLib == openImajFD) {
    		openImajFaceDet = new HaarCascadeDetector();
    	} else {
    	    System.out.println("Loading OpenCV and FaceDetector");
    	    nu.pattern.OpenCV.loadShared();
    		File haarFile = new File("haarcascade_frontalface_alt.xml");
    		openCVFaceDet = new CascadeClassifier(haarFile.getAbsolutePath());
    		System.out.println("Loaded OpenCV and FaceDetector");
    	}
    	choosenFD = faceDectLib;
    }
        
	@Override
	public void prepare() {
		setupFaceDetectionLib(openCVFD);
	}
	
	@Override
	public void execute() {
		int PicProcessController = 0;
	    final int PROCESS_FREQ = 1;
		
	    while (!Thread.currentThread().isInterrupted()) {
			InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(getTaskID());
	        if(pktRecv!=null){
	            byte[] frame = pktRecv.complexContent;
	            System.out.println("FACE DETECTOR RECEIVES A FRAME, "+ getTaskID());
	            
	            PicProcessController++;
	            if(PicProcessController == PROCESS_FREQ) {
		            // read byte frame into image
		            BufferedImage img = null;       
		            try {
						img = ImageIO.read(new ByteArrayInputStream(frame));
					} catch (IOException e) {
						e.printStackTrace();
					}
	            
		            // detect faces
		            List<BufferedImage> faces = new ArrayList<>();        
	            	switch(choosenFD) {
	            		case openImajFD: 
			            	FImage fimage = ImageUtilities.createFImage(img);
			            	long startTime1 = System.nanoTime();
			            	List <DetectedFace> detFaces = openImajFaceDet.detectFaces(fimage);
			            	long endTime1 = System.nanoTime();
			            	System.out.println("Time:" + (endTime1 - startTime1)/1000000000.0);
			            	for (DetectedFace detFace: detFaces) {
		                    	BufferedImage face = ImageUtilities.createBufferedImage(detFace.getFacePatch());
		                    	faces.add(face);
		                    }
	            		case openCVFD:	           
	            			Mat matImage = new Mat(img.getHeight(), img.getWidth(), CvType.CV_8UC3);
	            			byte[] imageData = ((DataBufferByte) img.getRaster().getDataBuffer()).getData();
	            			matImage.put(0, 0, imageData);	            			            
	            			MatOfRect matFaceList = new MatOfRect();	            	            	
	            			long startTime2 = System.nanoTime();
	            			openCVFaceDet.detectMultiScale(matImage, matFaceList);
	            			long endTime2 = System.nanoTime();	            			
	            			System.out.println("Time:" + (endTime2 - startTime2)/1000000000.0);
	            			for(Rect faceRectangle: matFaceList.toList()) {
	            				Mat faceImage = matImage.submat(faceRectangle);
	            				BufferedImage face = (BufferedImage) HighGui.toBufferedImage(faceImage);
	            				faces.add(face);
	            			}	            		
	            	}
	            	
		            // transmit detected faces
                    for (BufferedImage transFace: faces) {
                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        try {
							ImageIO.write(transFace, "jpg", outputStream);
							outputStream.flush();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
                        byte[] imageByteArray = outputStream.toByteArray();          
                        InternodePacket pktSend = new InternodePacket();
                        pktSend.type = InternodePacket.TYPE_DATA;
                        pktSend.fromTask = getTaskID();
                        pktSend.complexContent = imageByteArray;
                        String component = MyFaceRecognizer.class.getName();
                        try {
                            MessageQueues.emit(pktSend, getTaskID(), component);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("Faces detected and sent to face recognizer, " + System.nanoTime());
   
                    PicProcessController = 0;
	            }
	        }
        }
	}
	
	@Override
	public void postExecute() {
		if(openImajFaceDet!=null)
			openImajFaceDet = null;
		if(openCVFaceDet!=null)
			openCVFaceDet = null;
	}
}
