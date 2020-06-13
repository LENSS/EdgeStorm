package amvp;

import java.util.Comparator;

public class Model {
	public String type;
	public String baseType;
	public int frozenPoint;
	public double accuracy;
	
	public Model(String type, String baseType, int frozenPoint, double accuracy) {
		this.type = type;
		this.baseType = baseType;
		this.frozenPoint = frozenPoint;
		this.accuracy = accuracy;
	}
	
	public static Comparator<Model> accuracyComparator = new Comparator<Model>(){
		public int compare(Model m1, Model m2) { // descending order
			if (m1.accuracy > m2.accuracy) {
				return -1;
			} else if(m1.accuracy < m2.accuracy) {
				return 1;
			} else {
				return 0;
			}
		}
	};
}
