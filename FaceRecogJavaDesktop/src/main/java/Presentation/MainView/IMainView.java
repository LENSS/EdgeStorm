package Presentation.MainView;

import Presentation.IView;
import View.Controls.FaceControlView;

public interface IMainView extends IView<IMainViewPresenter> {
  void addFaceControlView(FaceControlView faceControlView);
  void clearFaces();
}
