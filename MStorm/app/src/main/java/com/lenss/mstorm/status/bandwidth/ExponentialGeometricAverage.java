package com.lenss.mstorm.status.bandwidth;

/**
 * Created by cmy on 1/13/17.
 */
public class ExponentialGeometricAverage {

    private final double mDecayConstant;
    private final int mCutover;
    private double mValue = -1;
    private int mCount;

    public ExponentialGeometricAverage(double decayConstant) {
        mDecayConstant = decayConstant;
        mCutover = (decayConstant == 0.0) ? Integer.MAX_VALUE : (int) Math.ceil(1/decayConstant);
    }

    public void addMeasurement(double measurement) {
        double keepConstant = 1 - mDecayConstant;
        if (mCount > mCutover) {
            mValue = Math.exp(keepConstant * Math.log(mValue) + mDecayConstant * Math.log(measurement));
        } else if (mCount > 0) {
            double retained = keepConstant * mCount / (mCount + 1.0);
            double newcomer = 1.0 - retained;
            mValue = Math.exp(retained * Math.log(mValue) + newcomer * Math.log(measurement));
        } else {
            mValue = measurement;
        }
        mCount++;
    }

    public double getAverage() {
        return mValue;
    }

    public void reset() {
        mValue = -1.0;
        mCount = 0;
    }
}
