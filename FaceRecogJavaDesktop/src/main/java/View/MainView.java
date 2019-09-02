package View;

import Presentation.MainView.IMainView;
import Presentation.MainView.IMainViewPresenter;
import View.Controls.FaceControlView;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.AnchorPane;
import javafx.stage.Stage;

import javax.swing.text.html.ListView;
import java.io.File;

public class MainView implements IMainView {
  private IMainViewPresenter _presenter;

  @Override
  public void setPresenter(IMainViewPresenter presenter) {
    if (_presenter==null)
      _presenter = presenter;
  }

  @Override
  public IMainViewPresenter getPresenter() {
    return _presenter;
  }

  private Stage _stage;
  private AnchorPane _anchorPane;
  private javafx.scene.control.ListView<FaceControlView> _faceListView;

  public MainView(Stage stage){
    _stage = stage;

    Button btnOpen = new Button();
    btnOpen.setLayoutX(14);
    btnOpen.setLayoutY(10);
    btnOpen.setText("Open");

    Button btnSave = new Button();
    btnSave.setLayoutX(74);
    btnSave.setLayoutY(10);
    btnSave.setText("Save");

    btnOpen.setOnMouseClicked(event -> {
      File file = _presenter.showFileDialogChooser();
      if (file!=null)
        _presenter.extractFaces(file);
    });


    btnSave.setOnMouseClicked(event -> {
        _presenter.saveTrainedFacesToDisk();
    });

    _faceListView = new javafx.scene.control.ListView<>();
    _faceListView.setLayoutY(45);
    _faceListView.setPrefHeight(350);
    _faceListView.setPrefWidth(600);

    _anchorPane = new AnchorPane();
    _anchorPane.prefWidth(400);
    _anchorPane.prefHeight(600);

    _anchorPane.getChildren().add(btnOpen);
    _anchorPane.getChildren().add(btnSave);
    _anchorPane.getChildren().add(_faceListView);
  }

  @Override
  public void show() {
    _stage.setTitle("Face Detector");

    Scene scene = new Scene(_anchorPane,600,400);
    _stage.setScene(scene);
    _stage.show();
  }

  @Override
  public void addFaceControlView(FaceControlView faceControlView) {
    _faceListView.getItems().add(faceControlView);
  }

  @Override
  public void clearFaces() {
    _faceListView.getItems().clear();
  }
}
