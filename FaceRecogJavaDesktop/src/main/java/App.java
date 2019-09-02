import Presentation.IApplicationController;
import View.ApplicationController;
import View.Controls.FaceControlView;
import javafx.application.Application;
import javafx.stage.Stage;

import java.io.File;

public class App extends Application {
  @Override
  public void start(Stage primaryStage) throws Exception {
    IApplicationController applicationController = new ApplicationController(primaryStage);
    applicationController.showMainView();
  }
}
