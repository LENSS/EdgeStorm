package Presentation.MainView;

import Presentation.IPresenter;

import java.io.File;

public interface IMainViewPresenter extends IPresenter<IMainView> {
  void extractFaces(File imageFilePath);
  File showFileDialogChooser();
  void saveTrainedFacesToDisk();
}
