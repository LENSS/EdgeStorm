package com.lenss.amvp.feature;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Quantizer {

    Logger logger = Logger.getLogger("Quantizer");

    public float minValue = Float.MAX_VALUE;

    public float maxValue = Float.MIN_VALUE;

    public ByteBuffer quantize(ByteBuffer input, int numOfbits){
        int capacity = input.capacity()/4;
        ByteBuffer output;

        for(int i=0; i < capacity; i++){
            float value = input.getFloat();
            minValue = Math.min(value, minValue);
            maxValue = Math.max(value, maxValue);
        }
        input.rewind();

        if(numOfbits == 16){
            output = ByteBuffer.allocateDirect(capacity*2);
            for(int i=0; i<capacity; i++){
                float floatValue = input.getFloat();
                short value = (short) Math.round((floatValue-minValue)/(maxValue-minValue)*65535);
                output.putShort(value);
            }
        } else if(numOfbits == 8){
            output = ByteBuffer.allocateDirect(capacity);
            for(int i=0; i<capacity; i++){
                float floatValue = input.getFloat();
                byte value = (byte) Math.round((floatValue-minValue)/(maxValue-minValue)*255);
                output.put(value);
            }
        } else if(numOfbits == 4){
            output = ByteBuffer.allocateDirect(capacity/2);
            for(int i=0; i<capacity; i=i+2){
                int value1 = Math.round((input.getFloat()-minValue)/(maxValue-minValue)*15);
                int value2 = Math.round((input.getFloat()-minValue)/(maxValue-minValue)*15);
                byte value = (byte)(value1 * 16 + value2);
                output.put(value);
            }
        } else { // numOfbits = 2
            output = ByteBuffer.allocateDirect(capacity/4);
            for(int i=0; i<capacity; i=i+4){
                int value1 = Math.round((input.getFloat()-minValue)/(maxValue-minValue)*3);
                int value2 = Math.round((input.getFloat()-minValue)/(maxValue-minValue)*3);
                int value3 = Math.round((input.getFloat()-minValue)/(maxValue-minValue)*3);
                int value4 = Math.round((input.getFloat()-minValue)/(maxValue-minValue)*3);
                byte value = (byte)(value1 * 64 + value2 * 16 + value3 * 4 + value4);
                output.put(value);
            }
        }
        return output;
    }

    public ByteBuffer dequantize(ByteBuffer input, int numOfbits, float minValue, float maxValue){
        int capacity = input.capacity();
        ByteBuffer output;

        if(numOfbits == 16){
            output = ByteBuffer.allocateDirect(capacity*2);
            for(int i=0; i<capacity/2; i++){
                short shortValue = input.getShort();
                float value = (shortValue>=0)?(maxValue-minValue)/65535*shortValue+minValue:(maxValue-minValue)/65535*(shortValue+65536)+minValue;
                output.putFloat(value);
            }
        } else if(numOfbits == 8){
            output = ByteBuffer.allocateDirect(capacity*4);
            for(int i=0; i<capacity; i++){
                byte byteValue = input.get();
                float value = (byteValue>=0)? (maxValue-minValue)/255*byteValue+minValue:(maxValue-minValue)/255*(byteValue+256)+minValue;
                output.putFloat(value);
            }
        } else if(numOfbits == 4){
            output = ByteBuffer.allocateDirect(capacity*8);
            for(int i=0; i<capacity; i++){
                byte value = input.get();
                int value1 = (value & 0xf0) >> 4;
                int value2 = value & 0x0f;
                float fvalue1 = (maxValue-minValue)/15*value1+minValue;
                float fvalue2 = (maxValue-minValue)/15*value2+minValue;
                output.putFloat(fvalue1);
                output.putFloat(fvalue2);
            }
        } else {  // numOfbits = 2
            output = ByteBuffer.allocateDirect(capacity*16);
            for(int i=0; i<capacity; i++){
                byte value = input.get();
                int value1 = (value & 0xc0) >> 6;
                int value2 = (value & 0x30) >> 4;
                int value3 = (value & 0x0c) >> 2;
                int value4 = value & 0x03;
                float fvalue1 = (maxValue-minValue)/3*value1+minValue;
                float fvalue2 = (maxValue-minValue)/3*value2+minValue;
                float fvalue3 = (maxValue-minValue)/3*value3+minValue;
                float fvalue4 = (maxValue-minValue)/3*value4+minValue;
                output.putFloat(fvalue1);
                output.putFloat(fvalue2);
                output.putFloat(fvalue3);
                output.putFloat(fvalue4);
            }
        }
        return output;
    }
}
