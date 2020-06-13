package amvp;

import java.util.Comparator;

public class SplittedModel {
	public String type;
	public String baseType;
	public int frozenPoint;
	public int splitPoint;
	public int part;	// 1 or 2
	public double latency;	// unit: ms
	public double memorySize; // unit: MB
	public double featureSize; // unit: KB
	
	public SplittedModel(String type, String baseType, int frozenPoint, int splitPoint, int part, double latency, double memorySize, double featureSize) {
		this.type = type;
		this.baseType = baseType;
		this.frozenPoint = frozenPoint;
		this.splitPoint = splitPoint;
		this.part = part;
		this.latency = latency;
		this.memorySize = memorySize;
		this.featureSize = featureSize;
	}
}

