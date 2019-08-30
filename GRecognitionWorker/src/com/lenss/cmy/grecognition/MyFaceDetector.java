package com.lenss.cmy.grecognition;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import org.bytedeco.javacpp.avformat.AVInputFormat.Create_device_capabilities_AVFormatContext_AVDeviceCapabilitiesQuery;
import org.openimaj.image.ImageUtilities;
import org.openimaj.image.processing.face.detection.DetectedFace;
import org.openimaj.image.processing.face.detection.HaarCascadeDetector;
import org.openimaj.math.geometry.shape.Rectangle;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Processor;

import gov.sandia.cognition.math.ClosedFormDifferentiableEvaluator;

public class MyFaceDetector extends Processor {  
	private final int OFD = 1;
	private HaarCascadeDetector openImajFaceDet = null;
	
	private final int CFD = 2;
	private 
	
	@Override
	public void prepare() {
		openImajFaceDet = new HaarCascadeDetector();
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
	            
	            // read byte frame into image
	            BufferedImage img = null;       
	            try {
					img = ImageIO.read(new ByteArrayInputStream(frame));
				} catch (IOException e) {
					e.printStackTrace();
				}
	            
	            // detect faces
	            Integer face_detected = 0;
	            List <DetectedFace> faces = null;
	            List<BufferedImage> bmfaces = new ArrayList<>();
	            PicProcessController++;
	            if(PicProcessController == PROCESS_FREQ) {
	            	long startTime = System.currentTimeMillis();
	            	faces = openImajFaceDet.detectFaces(ImageUtilities.createFImage(img));
	            	long endTime = System.currentTimeMillis();
                    System.out.println(("Openimaj face detection costs: " + String.valueOf((endTime - startTime) / 1000f) + " sec"));
	            	if(faces!=null)
	            		face_detected = faces.size();
                    for (int j = 0; j < face_detected; ++j) {
                    	Rectangle rec = faces.get(j).getBounds();
                    	BufferedImage bmface = null;
                    	
                        try {
                            bmface = img.getSubimage(Math.round(rec.x), Math.round(rec.y), 
                            		Math.round(rec.width), Math.round(rec.height));
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        
                        if (bmface != null) {
                            bmfaces.add(bmface);
                        }
                    }
                    PicProcessController = 0;
	            }
	            
	            // transmit detected faces
	            int bmfaceNum = bmfaces.size();
                if(bmfaceNum > 0){
                    for (int j = 0; j < bmfaceNum; ++j) {
                    	BufferedImage bmface = bmfaces.get(j);
                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        try {
							ImageIO.write(bmface, "jpg", outputStream);
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
                }
	        }
        }
	}
	
}
