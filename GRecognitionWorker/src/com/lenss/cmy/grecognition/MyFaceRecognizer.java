package com.lenss.cmy.grecognition;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.openimaj.image.processing.face.recognition.EigenFaceRecogniser;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Processor;

public class MyFaceRecognizer extends Processor {
    
	private EigenFaceRecogniser mFaceRec;
    
	@Override
	public void execute() {
        while (!Thread.currentThread().isInterrupted()) {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(getTaskID());
            if(pktRecv != null){
                byte[] frame = pktRecv.complexContent;
	            System.out.println("FACE RECOGNIZER RECEIVES A FRAME, "+ getTaskID());
	            
	            // read byte frame into image
	            BufferedImage img = null;       
	            try {
					img = ImageIO.read(new ByteArrayInputStream(frame));
				} catch (IOException e) {
					e.printStackTrace();
				}
            }
        }
	}
	
}
