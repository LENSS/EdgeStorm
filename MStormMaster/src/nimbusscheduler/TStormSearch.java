package nimbusscheduler;

import Jama.Matrix;

public class TStormSearch {
	Matrix devPropDelayGraph;
	Matrix devTransDelayGraph;
	Matrix devEnergyPerbitGraph;
	Matrix topOutputRateGraph; 
	Matrix topPktAvgSizeGraph;
	Matrix topPktTotalSizeGraph;
	int[] constraints;
	double[] batteries;
	int rowNum = 0;
	int colNum = 0;
	
    Matrix bestSchedule;
    double bestMetric;
    
	final double balanceRatio = 2.0;
	double balanceIndex;
	
	public TStormSearch(double[][] d2dPropDelay, double[][] d2dTransDelay, double[][] d2dEnergyPerbit,
			 double[][] t2tOutputRate, double[][] t2tPktAvgSize, int[] exptExecutorsOfDevice, double[] batt){		
		devPropDelayGraph = new Matrix(d2dPropDelay);
		devTransDelayGraph = new Matrix(d2dTransDelay);
		devEnergyPerbitGraph = new Matrix(d2dEnergyPerbit);
		topOutputRateGraph = new Matrix(t2tOutputRate);
		topPktAvgSizeGraph = new Matrix(t2tPktAvgSize);
		topPktTotalSizeGraph = topOutputRateGraph.arrayTimes(topPktAvgSizeGraph);
		constraints = exptExecutorsOfDevice;
		batteries = batt;
		rowNum = topOutputRateGraph.getRowDimension();
		colNum = devPropDelayGraph.getRowDimension();
		balanceIndex = balanceRatio * rowNum / colNum;
	}
	
	public TStormSearch(double[][] d2dPropDelay, double[][] d2dTransDelay, double[][] d2dEnergyPerbit, 
            double[][] mt2tOutputRate, double[][] mt2tPktAvgSize){
		devPropDelayGraph = new Matrix(d2dPropDelay);
		devTransDelayGraph = new Matrix(d2dTransDelay);
		devEnergyPerbitGraph = new Matrix(d2dEnergyPerbit);
		topOutputRateGraph = new Matrix(mt2tOutputRate);
		topPktAvgSizeGraph = new Matrix(mt2tPktAvgSize);
		topPktTotalSizeGraph = topOutputRateGraph.arrayTimes(topPktAvgSizeGraph);
	}
	
	public Matrix search(){
		bestSchedule = new Matrix(rowNum,colNum);
		
		int [] sortedTasks = sortTaskByTrafficInDescendingOrder(calTrafficsOfTasks());  // sort tasks according to the sum of input and output traffics in a descending order
		
		for (int i =0; i< rowNum; i++){
			int taskID = sortedTasks[i];
			int dev = 0;
			if (i==0){
				dev = findDevWithMaxResource();
			}
			else{
				dev = findDevWithMinTrafficIncresement(calIncreaseTrafficsOfTask(taskID));
			}
			bestSchedule.set(taskID, dev, 1);
		}
		return bestSchedule;
	}
	
	public double getInput(int taskID){
		double input = 0.0;
		for (int i = 0; i< rowNum; i++){
			input += topPktTotalSizeGraph.get(i, taskID);
		}
		return input;
	}
	
	public double getOutput(int taskID){
		double output = 0.0;
		for (int i = 0; i< rowNum; i++){
			output += topPktTotalSizeGraph.get(taskID, i);
		}
		return output;
	}
	
	public double[] calTrafficsOfTasks(){
		double[] taskTraffics = new double[rowNum];
		for (int i =0; i< rowNum; i++){
			taskTraffics[i] = getInput(i)+getOutput(i);
		}
		return taskTraffics;
	}
	
	public int[] sortTaskByTrafficInDescendingOrder(double[] taskTraffics){
		int[] sortedTasks = new int[rowNum];
		for(int i = 0; i<rowNum; i++){
			int maxIndex = 0;
			double maxTraffic = taskTraffics[0];
			for (int j=0; j<rowNum; j++){
				if(taskTraffics[j]>maxTraffic){
					maxIndex = j;
					maxTraffic = taskTraffics[j];
				}
			}
			sortedTasks[i] = maxIndex;
			taskTraffics[maxIndex]=-1.0;
		}
		
		return sortedTasks;
	}
	
	
	public int findDevWithMaxResource(){
		int maxIndex = 0;
		double maxCap = constraints[0];
		for (int i=1; i<colNum; i++){
			if(constraints[i]>maxCap){
				maxIndex = i;
				maxCap = constraints[i];
			}
		}
		return maxIndex;
	}
	
	public int findDevWithMinTrafficIncresement(double[] increaseTraffics){
		int minIndex = 0;
		double minIncrease = increaseTraffics[0];
		for (int i=0; i<colNum; i++){
			if(increaseTraffics[i]<minIncrease){
				minIndex = i;
				minIncrease = increaseTraffics[i];
			}
		}
		return minIndex;
	}
	
	public double[] calIncreaseTrafficsOfTask(int taskID){		
		double[] increaseTrafficsOfTask = new double[colNum];
		for (int i =0; i< colNum; i++){
			increaseTrafficsOfTask[i] = calIncreasedTraffics(taskID,i);
		}
		return increaseTrafficsOfTask;
	}
	
	public double calIncreasedTraffics(int taskID, int devID){
		double increaseTraffics = 0.0;
		if(getDevUsage(devID)+1>constraints[devID] || (getDevUsage(devID) + 1) > balanceIndex)
			increaseTraffics = Double.MAX_VALUE;
		else {
			for (int i = 0; i< rowNum; i++){	// input
				if(topPktTotalSizeGraph.get(i,taskID)>0 && judgeIfAssigned(i)>0 && bestSchedule.get(i,devID)==0){	// assign to different nodes
					increaseTraffics += topPktTotalSizeGraph.get(i,taskID);
				}
			}
			for (int i = 0; i< rowNum; i++){	// output
				if(topPktTotalSizeGraph.get(taskID,i)>0 && judgeIfAssigned(i)>0 && bestSchedule.get(i,devID)==0){	// assign to different nodes
					increaseTraffics += topPktTotalSizeGraph.get(taskID,i);
				}
			}
		}
		return increaseTraffics;
	}
	
	public double getDevUsage(int devID){
		double usage = 0.0;
		for (int i=0; i< rowNum; i++){
			usage += bestSchedule.get(i, devID);
		}
		return usage;
	}
	
	public double judgeIfAssigned(int taskID){
		double assigned = 0.0;
		for (int i=0; i< colNum; i++){
			assigned += bestSchedule.get(taskID, i);
		}
		return assigned;
	}

	public double calculateDelay(Matrix allocation){
		Matrix dataMatrix1 = topOutputRateGraph.times(allocation);
		Matrix commMatrix1 = allocation.times(devPropDelayGraph);
		Matrix costMatrix1 = dataMatrix1.arrayTimes(commMatrix1);
		
		Matrix trafficSizeMatrix = topOutputRateGraph.arrayTimes(topPktAvgSizeGraph);
		Matrix dataMatrix2 = trafficSizeMatrix.times(allocation);
		Matrix commMatrix2 = allocation.times(devTransDelayGraph);
		Matrix costMatrix2 = dataMatrix2.arrayTimes(commMatrix2);
		
		Matrix costMatrix = costMatrix1.plus(costMatrix2);
		
		double [][] matrix = costMatrix.getArray();
		int matrixRow = costMatrix.getRowDimension();
		int matrixCol = costMatrix.getColumnDimension();
		double metric = 0.0;
		for (int i = 0; i < matrixRow; i++)
	    {
	        for (int j = 0; j < matrixCol; j++)
	        {
	        	metric += matrix[i][j];
	        }
	    }
		return metric;	
	}
	
	public double calculateEnergy(Matrix allocation){	
		Matrix trafficSizeMatrix = topOutputRateGraph.arrayTimes(topPktAvgSizeGraph);
		Matrix dataMatrix = trafficSizeMatrix.times(allocation);
		Matrix energyMatrix = allocation.times(devEnergyPerbitGraph);
		Matrix costMatrix = dataMatrix.arrayTimes(energyMatrix);
		double [][] matrix = costMatrix.getArray();
		int matrixRow = costMatrix.getRowDimension();
		int matrixCol = costMatrix.getColumnDimension();
		double metric = 0.0;
		for (int i = 0; i < matrixRow; i++)
	    {
	        for (int j = 0; j < matrixCol; j++)
	        {
	        	metric += matrix[i][j];
	        }
	    }
		return metric;		
	}
}
