package Models;

import java.io.Serializable;

public class PersonModel implements Serializable{
  private double[] _faceFeatureArray;
  public double[] get_faceFeatureArray(){
    return _faceFeatureArray;
  }
  public void set_faceFeatureArray(double[] faceFeatureArray){
    _faceFeatureArray = faceFeatureArray;
  }

  private String _personName;
  public String get_personName() {
    return _personName;
  }
  public void set_personName(String _personName) {
    this._personName = _personName;
  }

  public PersonModel(){}
  public PersonModel(String personName, double[] faceFeatureArray){
    _personName = personName;
    _faceFeatureArray = faceFeatureArray;
  }
}
