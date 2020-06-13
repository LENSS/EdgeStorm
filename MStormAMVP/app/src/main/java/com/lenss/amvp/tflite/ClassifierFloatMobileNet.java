/* Copyright 2019 The TensorFlow Authors. All Rights Reserved.

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
import android.os.SystemClock;
import android.os.Trace;
import android.support.v4.graphics.drawable.IconCompat;

import com.lenss.amvp.env.Logger;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;


/** This TensorFlowLite classifier works with the float MobileNet model. */
public class ClassifierFloatMobileNet extends Classifier {
  private static final Logger LOGGER = new Logger();

  /** MobileNet requires additional normalization of the used input. */
  private static final float IMAGE_MEAN = 127.5f;
  private static final float IMAGE_STD = 127.5f;

  /**
   * An array to hold inference results, to be feed into Tensorflow Lite as outputs. This isn't part
   * of the super class, because we need a primitive array here.
   */
  private float[][] labelProbArray = null;

  /**
   * Initializes a {@code ClassifierFloatMobileNet}.
   *
   * @param activity
   */
  public ClassifierFloatMobileNet(Activity activity, String modelName, String modelVariant, Part part, Device device, int numThreads)
      throws IOException {
    super(activity, modelName, modelVariant, part, device, numThreads);

    if(part==Part.WHOLE || part==Part.SECOND_HALF){
      labelProbArray = new float[1][getNumLabels()];
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
    return name +".tflite";
    //return "model_gender.tflite";
    //return "model_emotion.tflite";
  }

  @Override
  protected String getLabelPath(String name) {
    return "labels_" + name + ".txt";
    //return "labels_gender.txt";
    //return "labels_emotion.txt";
    //return "labels.txt";
  }

  @Override
  protected int getNumBytesPerChannel() {
    return 4; // Float.SIZE / Byte.SIZE;
  }

  @Override
  protected void addPixelValue(int pixelValue) {
    imgData.putFloat((((pixelValue >> 16) & 0xFF) - IMAGE_MEAN) / IMAGE_STD);
    imgData.putFloat((((pixelValue >> 8) & 0xFF) - IMAGE_MEAN) / IMAGE_STD);
    imgData.putFloat(((pixelValue & 0xFF) - IMAGE_MEAN) / IMAGE_STD);
  }

  @Override
  protected float getProbability(int labelIndex) {
    return labelProbArray[0][labelIndex];
  }

  @Override
  protected void setProbability(int labelIndex, Number value) {
    labelProbArray[0][labelIndex] = value.floatValue();
  }

  @Override
  protected float getNormalizedProbability(int labelIndex) {
    return labelProbArray[0][labelIndex];
  }

  @Override
  protected void runInference() {
    LOGGER.i("runInferenceImg2FM===img===rem===1===" + imgData.remaining());
    LOGGER.i("runInferenceImg2FM===img===lim===1===" + imgData.limit());
    LOGGER.i("runInferenceImg2FM===img===cap===1===" + imgData.capacity());
    LOGGER.i("runInferenceImg2FM===img===pos===1===" + imgData.position());

    tflite.run(imgData, labelProbArray);

    LOGGER.i("runInferenceImg2FM===img===rem===2===" + imgData.remaining());
    LOGGER.i("runInferenceImg2FM===img===lim===2===" + imgData.limit());
    LOGGER.i("runInferenceImg2FM===img===cap===2===" + imgData.capacity());
    LOGGER.i("runInferenceImg2FM===img===pos===2===" + imgData.position());
  }

  @Override
  protected void runInferenceImg2FM(){
    LOGGER.i("runInferenceImg2FM===img===rem===1===" + imgData.remaining());
    LOGGER.i("runInferenceImg2FM===img===lim===1===" + imgData.limit());
    LOGGER.i("runInferenceImg2FM===img===cap===1===" + imgData.capacity());
    LOGGER.i("runInferenceImg2FM===img===pos===1===" + imgData.position());

    LOGGER.i("runInferenceImg2FM===feature===rem===1===" + featureMaps.remaining());
    LOGGER.i("runInferenceImg2FM===feature===lim===1===" + featureMaps.limit());
    LOGGER.i("runInferenceImg2FM===feature===cap===1===" + featureMaps.capacity());
    LOGGER.i("runInferenceImg2FM===feature===pos===1===" + featureMaps.position());

    featureMaps.rewind();
    tflite.run(imgData, featureMaps);

    LOGGER.i("runInferenceImg2FM===img===rem===2===" + imgData.remaining());
    LOGGER.i("runInferenceImg2FM===img===lim===2===" + imgData.limit());
    LOGGER.i("runInferenceImg2FM===img===cap===2===" + imgData.capacity());
    LOGGER.i("runInferenceImg2FM===img===pos===2===" + imgData.position());

    LOGGER.i("runInferenceImg2FM===feature===rem===2===" + featureMaps.remaining());
    LOGGER.i("runInferenceImg2FM===feature===lim===2===" + featureMaps.limit());
    LOGGER.i("runInferenceImg2FM===feature===cap===2===" + featureMaps.capacity());
    LOGGER.i("runInferenceImg2FM===feature===pos===2===" + featureMaps.position());
  }

  @Override
  protected void runInferenceFM2Prob(){
    Object[] inputs = new Object[2];
    inputs[0] = imgData;
    inputs[1] = featureMaps;

    LOGGER.i("runInferenceFM2Prob===feature===rem===2===" + featureMaps.remaining());
    LOGGER.i("runInferenceFM2Prob===feature===lim===2===" + featureMaps.limit());
    LOGGER.i("runInferenceFM2Prob===feature===cap===2===" + featureMaps.capacity());
    LOGGER.i("runInferenceFM2Prob===feature===pos===2===" + featureMaps.position());

    Map<Integer, Object> outputs = new HashMap();
    outputs.put(0, labelProbArray);
    tflite.runForMultipleInputsOutputs(inputs, outputs);
  }
}
