package amvp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.text.DefaultStyledDocument.ElementSpec;

public class ModelCatlog {
	ArrayList<Model> models = new ArrayList<>();
	ArrayList<SplittedModel> splittedModels = new ArrayList<>();
	
	public ModelCatlog() {
		// models for gender
		Model gender_mn_f45 =  new Model("gender", "mobileNetV2", 45, 89.6);
		Model gender_mn_f81 =  new Model("gender", "mobileNetV2", 81, 86.4);
		Model gender_mn_f116 = new Model("gender", "mobileNetV2", 116, 85.167);
		Model gender_mn_f143 = new Model("gender", "mobileNetV2", 143, 82.2);
		Model gender_rn_f39 =  new Model("gender", "resNet50V2", 39, 91.4);
		Model gender_rn_f85 =  new Model("gender", "resNet50V2", 85, 91.867);
		Model gender_rn_f153 = new Model("gender", "resNet50V2", 153, 89.833);
		Model gender_rn_f187 = new Model("gender", "resNet50V2", 187, 88.433);
		models.add(gender_mn_f45);
		models.add(gender_mn_f81);
		models.add(gender_mn_f116);
		models.add(gender_mn_f143);
		models.add(gender_rn_f39);
		models.add(gender_rn_f85);
		models.add(gender_rn_f153);
		models.add(gender_rn_f187);
		
		// models for emotion
		Model emotion_mn_f45 =  new Model("emotion", "mobileNetV2", 45, 88.867);
		Model emotion_mn_f81 =  new Model("emotion", "mobileNetV2", 81, 87.467);
		Model emotion_mn_f116 = new Model("emotion", "mobileNetV2", 116, 81.2);
		Model emotion_mn_f143 = new Model("emotion", "mobileNetV2", 143, 69.533);
		Model emotion_rn_f39 =  new Model("emotion", "resNet50V2", 39, 92.567);
		Model emotion_rn_f85 =  new Model("emotion", "resNet50V2", 85, 91.467);
		Model emotion_rn_f153 = new Model("emotion", "resNet50V2", 153, 87.3);
		Model emotion_rn_f187 = new Model("emotion", "resNet50V2", 187, 60.233);
		models.add(emotion_mn_f45);
		models.add(emotion_mn_f81);
		models.add(emotion_mn_f116);
		models.add(emotion_mn_f143);
		models.add(emotion_rn_f39);
		models.add(emotion_rn_f85);
		models.add(emotion_rn_f153);
		models.add(emotion_rn_f187);
		
		// models for age
		Model age_mn_f45 =  new Model("age", "mobileNetV2", 45, 85.422);
		Model age_mn_f81 =  new Model("age", "mobileNetV2", 81, 80.756);
		Model age_mn_f116 = new Model("age", "mobileNetV2", 116, 71.733);
		Model age_mn_f143 = new Model("age", "mobileNetV2", 143, 62.6);
		Model age_rn_f39 =  new Model("age", "resNet50V2", 39, 89.711);
		Model age_rn_f85 =  new Model("age", "resNet50V2", 85, 90.422);
		Model age_rn_f153 = new Model("age", "resNet50V2", 153, 83.689);
		Model age_rn_f187 = new Model("age", "resNet50V2", 187, 73.644);
		models.add(age_mn_f45);
		models.add(age_mn_f81);
		models.add(age_mn_f116);
		models.add(age_mn_f143);
		models.add(age_rn_f39);
		models.add(age_rn_f85);
		models.add(age_rn_f153);
		models.add(age_rn_f187);
		
		// common split models
		SplittedModel common_mn_s45_1 =  new SplittedModel("common", "mobileNetV2", -1, 45,  1, 51.868, 0.173312, 100.352);
		SplittedModel common_mn_s81_1 =  new SplittedModel("common", "mobileNetV2", -1, 81,  1, 65.057, 0.771328, 50.176);		
		SplittedModel common_mn_s116_1 = new SplittedModel("common", "mobileNetV2", -1, 116, 1, 85.083, 2.234624, 75.264);
		SplittedModel common_mn_s143_1 = new SplittedModel("common", "mobileNetV2", -1, 143, 1, 98.922, 5.459456, 31.36);			
		SplittedModel common_rn_s39_1 =  new SplittedModel("common", "resNet50V2",  -1, 39,  1, 216.288,  0.909312,  802.816);
		SplittedModel common_rn_s85_1 =  new SplittedModel("common", "resNet50V2",  -1, 85,  1, 446.443,  5.814272,  401.408);
		SplittedModel common_rn_s153_1 = new SplittedModel("common", "resNet50V2",  -1, 153, 1, 798.169,  34.29376,  200.704);
		SplittedModel common_rn_s187_1 = new SplittedModel("common", "resNet50V2",  -1, 187, 1, 1018.952, 94.226432, 401.408);
		splittedModels.add(common_mn_s45_1);
		splittedModels.add(common_mn_s81_1);
		splittedModels.add(common_mn_s116_1);
		splittedModels.add(common_mn_s143_1);
		splittedModels.add(common_rn_s39_1);
		splittedModels.add(common_rn_s85_1);
		splittedModels.add(common_rn_s153_1);
		splittedModels.add(common_rn_s187_1);
		
		// split modes for gender
		SplittedModel gender_mn_f45_s45_2 =   new SplittedModel("gender", "mobileNetV2",  45,  45, 2, 62.663, 12.538892, 0);
		SplittedModel gender_mn_f81_s45_2 =   new SplittedModel("gender", "mobileNetV2",  81,  45, 2, 62.663, 12.538892, 0);
		SplittedModel gender_mn_f116_s45_2 =  new SplittedModel("gender", "mobileNetV2", 116,  45, 2, 62.663, 12.538892, 0);
		SplittedModel gender_mn_f143_s45_2 =  new SplittedModel("gender", "mobileNetV2", 143,  45, 2, 62.663, 12.538892, 0);
		SplittedModel gender_mn_f81_s81_2 =   new SplittedModel("gender", "mobileNetV2",  81,  81, 2, 49.474, 11.940876, 0);
		SplittedModel gender_mn_f116_s81_2 =  new SplittedModel("gender", "mobileNetV2", 116,  81, 2, 49.474, 11.940876, 0);
		SplittedModel gender_mn_f143_s81_2 =  new SplittedModel("gender", "mobileNetV2", 143,  81, 2, 49.474, 11.940876, 0);
		SplittedModel gender_mn_f116_s116_2 = new SplittedModel("gender", "mobileNetV2", 116, 116, 2, 29.448, 10.47758, 0);
		SplittedModel gender_mn_f143_s116_2 = new SplittedModel("gender", "mobileNetV2", 143, 116, 2, 29.448, 10.47758, 0);
		SplittedModel gender_mn_f143_s143_2 = new SplittedModel("gender", "mobileNetV2", 143, 143, 2, 15.609, 7.252748, 0);
		SplittedModel gender_rn_f39_s39_2 =   new SplittedModel("gender", "resNet50V2",  39,  39, 2, 807.367, 98.60302, 0);
		SplittedModel gender_rn_f85_s39_2 =   new SplittedModel("gender", "resNet50V2",  85,  39, 2, 807.367, 98.60302, 0);
		SplittedModel gender_rn_f153_s39_2 =  new SplittedModel("gender", "resNet50V2", 153,  39, 2, 807.367, 98.60302, 0);
		SplittedModel gender_rn_f187_s39_2 =  new SplittedModel("gender", "resNet50V2", 187,  39, 2, 807.367, 98.60302, 0);
		SplittedModel gender_rn_f85_s85_2 =   new SplittedModel("gender", "resNet50V2",  85,  85, 2, 577.212, 93.69806, 0);
		SplittedModel gender_rn_f153_s85_2 =  new SplittedModel("gender", "resNet50V2", 153,  85, 2, 577.212, 93.69806, 0);
		SplittedModel gender_rn_f187_s85_2 =  new SplittedModel("gender", "resNet50V2", 187,  85, 2, 577.212, 93.69806, 0);
		SplittedModel gender_rn_f153_s153_2 = new SplittedModel("gender", "resNet50V2", 153, 153, 2, 225.486, 65.218572,0);
		SplittedModel gender_rn_f187_s153_2 = new SplittedModel("gender", "resNet50V2", 187, 153, 2, 225.486, 65.218572,0);
		SplittedModel gender_rn_f187_s187_2 = new SplittedModel("gender", "resNet50V2", 187, 187, 2, 4.703, 5.2859, 0);
		splittedModels.add(gender_mn_f45_s45_2);
		splittedModels.add(gender_mn_f81_s45_2);
		splittedModels.add(gender_mn_f116_s45_2);
		splittedModels.add(gender_mn_f143_s45_2);
		splittedModels.add(gender_mn_f81_s81_2);
		splittedModels.add(gender_mn_f116_s81_2);
		splittedModels.add(gender_mn_f143_s81_2);
		splittedModels.add(gender_mn_f116_s116_2);
		splittedModels.add(gender_mn_f143_s116_2);
		splittedModels.add(gender_mn_f143_s143_2);
		splittedModels.add(gender_rn_f39_s39_2);
		splittedModels.add(gender_rn_f85_s39_2);
		splittedModels.add(gender_rn_f153_s39_2);
		splittedModels.add(gender_rn_f187_s39_2);
		splittedModels.add(gender_rn_f85_s85_2);
		splittedModels.add(gender_rn_f153_s85_2);
		splittedModels.add(gender_rn_f187_s85_2);
		splittedModels.add(gender_rn_f153_s153_2);
		splittedModels.add(gender_rn_f187_s153_2);
		splittedModels.add(gender_rn_f187_s187_2);
		
		// split modes for emotion
		SplittedModel emotion_mn_f45_s45_2 =   new SplittedModel("emotion", "mobileNetV2",  45,  45, 2, 62.663, 12.538892, 0);
		SplittedModel emotion_mn_f81_s45_2 =   new SplittedModel("emotion", "mobileNetV2",  81,  45, 2, 62.663, 12.538892, 0);
		SplittedModel emotion_mn_f116_s45_2 =  new SplittedModel("emotion", "mobileNetV2", 116,  45, 2, 62.663, 12.538892, 0);
		SplittedModel emotion_mn_f143_s45_2 =  new SplittedModel("emotion", "mobileNetV2", 143,  45, 2, 62.663, 12.538892, 0);
		SplittedModel emotion_mn_f81_s81_2 =   new SplittedModel("emotion", "mobileNetV2",  81,  81, 2, 49.474, 11.940876, 0);
		SplittedModel emotion_mn_f116_s81_2 =  new SplittedModel("emotion", "mobileNetV2", 116,  81, 2, 49.474, 11.940876, 0);
		SplittedModel emotion_mn_f143_s81_2 =  new SplittedModel("emotion", "mobileNetV2", 143,  81, 2, 49.474, 11.940876, 0);
		SplittedModel emotion_mn_f116_s116_2 = new SplittedModel("emotion", "mobileNetV2", 116, 116, 2, 29.448, 10.47758, 0);
		SplittedModel emotion_mn_f143_s116_2 = new SplittedModel("emotion", "mobileNetV2", 143, 116, 2, 29.448, 10.47758, 0);
		SplittedModel emotion_mn_f143_s143_2 = new SplittedModel("emotion", "mobileNetV2", 143, 143, 2, 15.609, 7.252748, 0);
		SplittedModel emotion_rn_f39_s39_2 =   new SplittedModel("emotion", "resNet50V2",  39,  39, 2, 807.367, 98.60302, 0);
		SplittedModel emotion_rn_f85_s39_2 =   new SplittedModel("emotion", "resNet50V2",  85,  39, 2, 807.367, 98.60302, 0);
		SplittedModel emotion_rn_f153_s39_2 =  new SplittedModel("emotion", "resNet50V2", 153,  39, 2, 807.367, 98.60302, 0);
		SplittedModel emotion_rn_f187_s39_2 =  new SplittedModel("emotion", "resNet50V2", 187,  39, 2, 807.367, 98.60302, 0);
		SplittedModel emotion_rn_f85_s85_2 =   new SplittedModel("emotion", "resNet50V2",  85,  85, 2, 577.212, 93.69806, 0);
		SplittedModel emotion_rn_f153_s85_2 =  new SplittedModel("emotion", "resNet50V2", 153,  85, 2, 577.212, 93.69806, 0);
		SplittedModel emotion_rn_f187_s85_2 =  new SplittedModel("emotion", "resNet50V2", 187,  85, 2, 577.212, 93.69806, 0);
		SplittedModel emotion_rn_f153_s153_2 = new SplittedModel("emotion", "resNet50V2", 153, 153, 2, 225.486, 65.218572,0);
		SplittedModel emotion_rn_f187_s153_2 = new SplittedModel("emotion", "resNet50V2", 187, 153, 2, 225.486, 65.218572,0);
		SplittedModel emotion_rn_f187_s187_2 = new SplittedModel("emotion", "resNet50V2", 187, 187, 2, 4.703, 5.2859, 0);
		splittedModels.add(emotion_mn_f45_s45_2);
		splittedModels.add(emotion_mn_f81_s45_2);
		splittedModels.add(emotion_mn_f116_s45_2);
		splittedModels.add(emotion_mn_f143_s45_2);
		splittedModels.add(emotion_mn_f81_s81_2);
		splittedModels.add(emotion_mn_f116_s81_2);
		splittedModels.add(emotion_mn_f143_s81_2);
		splittedModels.add(emotion_mn_f116_s116_2);
		splittedModels.add(emotion_mn_f143_s116_2);
		splittedModels.add(emotion_mn_f143_s143_2);
		splittedModels.add(emotion_rn_f39_s39_2);
		splittedModels.add(emotion_rn_f85_s39_2);
		splittedModels.add(emotion_rn_f153_s39_2);
		splittedModels.add(emotion_rn_f187_s39_2);
		splittedModels.add(emotion_rn_f85_s85_2);
		splittedModels.add(emotion_rn_f153_s85_2);
		splittedModels.add(emotion_rn_f187_s85_2);
		splittedModels.add(emotion_rn_f153_s153_2);
		splittedModels.add(emotion_rn_f187_s153_2);
		splittedModels.add(emotion_rn_f187_s187_2);
		
		// split modes for age
		SplittedModel age_mn_f45_s45_2 =   new SplittedModel("age", "mobileNetV2",  45,  45, 2, 62.663, 12.538892, 0);
		SplittedModel age_mn_f81_s45_2 =   new SplittedModel("age", "mobileNetV2",  81,  45, 2, 62.663, 12.538892, 0);
		SplittedModel age_mn_f116_s45_2 =  new SplittedModel("age", "mobileNetV2", 116,  45, 2, 62.663, 12.538892, 0);
		SplittedModel age_mn_f143_s45_2 =  new SplittedModel("age", "mobileNetV2", 143,  45, 2, 62.663, 12.538892, 0);
		SplittedModel age_mn_f81_s81_2 =   new SplittedModel("age", "mobileNetV2",  81,  81, 2, 49.474, 11.940876, 0);
		SplittedModel age_mn_f116_s81_2 =  new SplittedModel("age", "mobileNetV2", 116,  81, 2, 49.474, 11.940876, 0);
		SplittedModel age_mn_f143_s81_2 =  new SplittedModel("age", "mobileNetV2", 143,  81, 2, 49.474, 11.940876, 0);
		SplittedModel age_mn_f116_s116_2 = new SplittedModel("age", "mobileNetV2", 116, 116, 2, 29.448, 10.47758, 0);
		SplittedModel age_mn_f143_s116_2 = new SplittedModel("age", "mobileNetV2", 143, 116, 2, 29.448, 10.47758, 0);
		SplittedModel age_mn_f143_s143_2 = new SplittedModel("age", "mobileNetV2", 143, 143, 2, 15.609, 7.252748, 0);
		SplittedModel age_rn_f39_s39_2 =   new SplittedModel("age", "resNet50V2",  39,  39, 2, 807.367, 98.60302, 0);
		SplittedModel age_rn_f85_s39_2 =   new SplittedModel("age", "resNet50V2",  85,  39, 2, 807.367, 98.60302, 0);
		SplittedModel age_rn_f153_s39_2 =  new SplittedModel("age", "resNet50V2", 153,  39, 2, 807.367, 98.60302, 0);
		SplittedModel age_rn_f187_s39_2 =  new SplittedModel("age", "resNet50V2", 187,  39, 2, 807.367, 98.60302, 0);
		SplittedModel age_rn_f85_s85_2 =   new SplittedModel("age", "resNet50V2",  85,  85, 2, 577.212, 93.69806, 0);
		SplittedModel age_rn_f153_s85_2 =  new SplittedModel("age", "resNet50V2", 153,  85, 2, 577.212, 93.69806, 0);
		SplittedModel age_rn_f187_s85_2 =  new SplittedModel("age", "resNet50V2", 187,  85, 2, 577.212, 93.69806, 0);
		SplittedModel age_rn_f153_s153_2 = new SplittedModel("age", "resNet50V2", 153, 153, 2, 225.486, 65.218572,0);
		SplittedModel age_rn_f187_s153_2 = new SplittedModel("age", "resNet50V2", 187, 153, 2, 225.486, 65.218572,0);
		SplittedModel age_rn_f187_s187_2 = new SplittedModel("age", "resNet50V2", 187, 187, 2, 4.703, 5.2859, 0);
		splittedModels.add(age_mn_f45_s45_2);
		splittedModels.add(age_mn_f81_s45_2);
		splittedModels.add(age_mn_f116_s45_2);
		splittedModels.add(age_mn_f143_s45_2);
		splittedModels.add(age_mn_f81_s81_2);
		splittedModels.add(age_mn_f116_s81_2);
		splittedModels.add(age_mn_f143_s81_2);
		splittedModels.add(age_mn_f116_s116_2);
		splittedModels.add(age_mn_f143_s116_2);
		splittedModels.add(age_mn_f143_s143_2);
		splittedModels.add(age_rn_f39_s39_2);
		splittedModels.add(age_rn_f85_s39_2);
		splittedModels.add(age_rn_f153_s39_2);
		splittedModels.add(age_rn_f187_s39_2);
		splittedModels.add(age_rn_f85_s85_2);
		splittedModels.add(age_rn_f153_s85_2);
		splittedModels.add(age_rn_f187_s85_2);
		splittedModels.add(age_rn_f153_s153_2);
		splittedModels.add(age_rn_f187_s153_2);
		splittedModels.add(age_rn_f187_s187_2);		
	}
	
	public Model secondTo(Model model, String type, String baseType) {
		// sort model by accuracy
		Collections.sort(models, Model.accuracyComparator);
		Model resModel = null;
		
		if(model != null) {
			for(Model m: models) {
				if(m.type.equals(type) && m.baseType.equals(baseType) && m.accuracy < model.accuracy) {
					resModel = m;
					break;
				}
			}
		} else {
			for(Model m: models) {
				if(m.type.equals(type) && m.baseType.equals(baseType)) {
					resModel = m;
					break;
				}
			}
		}
		return resModel;
	}
	
	public List<SplittedModel> getSplittedModels(List<Model> orgModels){
		// get splitting points
		List<Integer> frozenPoints = new ArrayList<>();
		for(Model m: orgModels) {
			frozenPoints.add(m.frozenPoint);
		}
		Collections.sort(frozenPoints);
		int splitPoint = frozenPoints.get(0);
		
		// get split models
		List<SplittedModel> setOfSplittedModels = new ArrayList<>();
		// find common parts
		String modelBaseType = orgModels.get(0).baseType;
		for(SplittedModel sm: splittedModels) {
			if(sm.baseType.equals(modelBaseType) && sm.type.equals("common") && sm.splitPoint == splitPoint) {
				setOfSplittedModels.add(sm);
			}
		}
		// find split second parts
		for(Model m: orgModels) {
			for(SplittedModel sm: splittedModels) {
				if(sm.type.equals(m.type) && sm.baseType.equals(m.baseType) && sm.frozenPoint==m.frozenPoint && sm.splitPoint == splitPoint) {
					setOfSplittedModels.add(sm);
				}
			}
		}
		return setOfSplittedModels;
	} 

	public Model searchModel(String type, String baseType, int frozenPoint) {
		Model model = null;
		for(Model m: models) {
			if(m.type.equals(type) && m.baseType.equals(baseType) && m.frozenPoint == frozenPoint) {
				model = m;
				break;
			}
		}
		return model;
	}
}
