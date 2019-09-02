package View.Controls;

import View.Controls.Events.IOnRecognizeEventListener;
import View.Controls.Events.IOnTrainEventListener;
import javafx.embed.swing.SwingFXUtils;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import org.omg.CORBA.TRANSACTION_REQUIRED;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FaceControlView extends AnchorPane {

  private ImageView _imageView;
  private TextField _txtPersonName;
  private File _faceImageFile;

  private List<IOnTrainEventListener> _trainEventList;
  public void addOnTrainListener(IOnTrainEventListener listener){
    _trainEventList.add(listener);
  }

  private List<IOnRecognizeEventListener> _recognizeEventList;
  public void addOnRecognizeListener(IOnRecognizeEventListener listener){_recognizeEventList.add(listener);}

  public FaceControlView(){
    _trainEventList = new ArrayList<>();
    _recognizeEventList = new ArrayList<>();

    this.minHeight(290.0);
    this.minWidth(200.0);
    this.prefHeight(290.0);
    this.prefWidth(200.0);
    this.maxHeight(290.0);
    this.maxWidth(200.0);

    _imageView = new ImageView();
    _imageView.setFitHeight(148.0);
    _imageView.setFitWidth(172.0);
    _imageView.setLayoutX(14.0);
    _imageView.setLayoutY(14.0);
    _imageView.setPickOnBounds(true);
    _imageView.setPreserveRatio(true);

    _txtPersonName = new TextField();
    _txtPersonName.setLayoutX(14);
    _txtPersonName.setLayoutY(170);
    _txtPersonName.setPrefHeight(30);
    _txtPersonName.setPrefWidth(174);

    Button btnTrain = new Button();
    btnTrain.setLayoutX(14);
    btnTrain.setLayoutY(210);
    btnTrain.setPrefWidth(174);
    btnTrain.setText("Train");
    btnTrain.setOnMouseClicked(event -> {
      for(IOnTrainEventListener listener : _trainEventList)
        listener.onTrain(_txtPersonName.getText(),_faceImageFile);
    });

    Button btnRecognize = new Button();
    btnRecognize.setLayoutX(14);
    btnRecognize.setLayoutY(250);
    btnRecognize.setPrefWidth(174);
    btnRecognize.setText("Recognize");
    btnRecognize.setOnMouseClicked(event -> {
      for(IOnRecognizeEventListener listener : _recognizeEventList)
        listener.onRecognize(this,_faceImageFile);
    });

    this.getChildren().add(_imageView);
    this.getChildren().add(_txtPersonName);
    this.getChildren().add(btnTrain);
    this.getChildren().add(btnRecognize);
  }

  public void setImage(File imageFilePath){
    BufferedImage faceImage = null;
    _faceImageFile =imageFilePath;
    try {
      faceImage = ImageIO.read(imageFilePath);
      _imageView.setImage(SwingFXUtils.toFXImage(faceImage,null));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void setPersonName(String name){
    _txtPersonName.setText(name);
  }
}
