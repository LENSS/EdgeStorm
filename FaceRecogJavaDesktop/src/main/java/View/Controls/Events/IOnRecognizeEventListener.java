package View.Controls.Events;

import View.Controls.FaceControlView;

import java.io.File;

public interface IOnRecognizeEventListener {
  void onRecognize(FaceControlView sender, File personImageFile);
}
