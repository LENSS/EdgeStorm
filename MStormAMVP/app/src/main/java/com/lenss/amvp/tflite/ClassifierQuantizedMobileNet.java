/* Copyright 2017 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

package com.lenss.amvp.tflite;

import android.app.Activity;
import android.graphics.Bitmap;

import org.jboss.netty.util.internal.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/** This TensorFlow Lite classifier works with the quantized MobileNet model. */
public class ClassifierQuantizedMobileNet extends Classifier {

  /**
   * An array to hold inference results, to be feed into Tensorflow Lite as outputs. This isn't part
   * of the super class, because we need a primitive array here.
   */
  private byte[][] labelProbArray = null;

  /**
   * A ByteBuffer to hold immediate feature maps, to be feed into Tensorflow Lite as outputs/inputs.
   */
  private ByteBuffer featureMaps = null;

  /**
   * Initializes a {@code ClassifierQuantizedMobileNet}.
   *
   * @param activity
   */
  public ClassifierQuantizedMobileNet(Activity activity, String modelName, String modelVariant, Part part, Device device, int numThreads)
      throws IOException {
    super(activity, modelName, modelVariant, part, device, numThreads);

    if(part==Part.WHOLE || part==Part.SECOND_HALF){
      labelProbArray = new byte[1][getNumLabels()];
    }

  }

  @Override
  public int getImageSizeX() {
    return 224;
  }

  @Override
  public int getImageSizeY() {
    return 224;
  }

  @Override
  protected String getModelPath(String name) {
    // you can download this file from
    // see build.gradle for where to obtain this file. It should be auto
    // downloaded into assets.
    return "mobilenet_v1_1.0_224_quant.tflite";
  }

  @Override
  protected String getLabelPath(String name) {
    return "labels.txt";
  }

  @Override
  protected int getNumBytesPerChannel() {
    // the quantized model uses a single byte only
    return 1;
  }

  @Override
  protected void addPixelValue(int pixelValue) {
    imgData.put((byte) ((pixelValue >> 16) & 0xFF));
    imgData.put((byte) ((pixelValue >> 8) & 0xFF));
    imgData.put((byte) (pixelValue & 0xFF));
  }

  @Override
  protected float getProbability(int labelIndex) {
    return labelProbArray[0][labelIndex];
  }

  @Override
  protected void setProbability(int labelIndex, Number value) {
    labelProbArray[0][labelIndex] = value.byteValue();
  }

  @Override
  protected float getNormalizedProbability(int labelIndex) {
    return (labelProbArray[0][labelIndex] & 0xff) / 255.0f;
  }

  @Override
  protected void runInference() {
    tflite.run(imgData, labelProbArray);
  }

  @Override
  protected void runInferenceImg2FM(){
    tflite.run(imgData, featureMaps);
  }

  @Override
  protected void runInferenceFM2Prob(){
    tflite.run(featureMaps, labelProbArray);
  }

}
