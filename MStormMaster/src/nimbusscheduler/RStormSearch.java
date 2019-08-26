package nimbusscheduler;

import Jama.Matrix;

public class RStormSearch {
	Matrix devPropDelayGraph;
	Matrix devTransDelayGraph;
	Matrix devEnergyPerbitGraph;
	Matrix topOutputRateGraph; 
	Matrix topPktAvgSizeGraph;
	int[] constraints;
	double[] batteries;
	int rowNum = 0;
	int colNum = 0;
	
    Matrix bestSchedule;
    double bestMetric;	
	
    public RStormSearch(double[][] d2dPropDelay, double[][] d2dTransDelay, double[][] d2dEnergyPerbit,
			 double[][] t2tOutputRate, double[][] t2tPktAvgSize, int[] exptExecutorsOfDevice, double[] batt){		
		devPropDelayGraph = new Matrix(d2dPropDelay);
		devTransDelayGraph = new Matrix(d2dTransDelay);
		devEnergyPerbitGraph = new Matrix(d2dEnergyPerbit);
		topOutputRateGraph = new Matrix(t2tOutputRate);
		topPktAvgSizeGraph = new Matrix(t2tPktAvgSize);
		constraints = exptExecutorsOfDevice;
		batteries = batt;
		rowNum = topOutputRateGraph.getRowDimension();
		colNum = devPropDelayGraph.getRowDimension();
	}
	
	public RStormSearch(double[][] d2dPropDelay, double[][] d2dTransDelay, double[][] d2dEnergyPerbit, 
           double[][] mt2tOutputRate, double[][] mt2tPktAvgSize){
		devPropDelayGraph = new Matrix(d2dPropDelay);
		devTransDelayGraph = new Matrix(d2dTransDelay);
		devEnergyPerbitGraph = new Matrix(d2dEnergyPerbit);
		topOutputRateGraph = new Matrix(mt2tOutputRate);
		topPktAvgSizeGraph = new Matrix(mt2tPktAvgSize);
	}
	
	public Matrix search(){
		bestSchedule = new Matrix(rowNum,colNum);
		
		int [] sortedTasks = sortTaskByTrafficInDescendingOrder(calTrafficsOfTasks());  // sort tasks according to the sum of input and output edges in a descending order
		
		for (int i =0; i< rowNum; i++){
			int taskID = sortedTasks[i];
			int dev = findDevByResourceAware();
			bestSchedule.set(taskID, dev, 1);
		}		
		return bestSchedule;
	}
	
	public double getInput(int taskID){
		double input = 0.0;
		for (int i = 0; i< rowNum; i++){
			input += (topOutputRateGraph.get(i, taskID)>0)?1:0;
		}
		return input;
	}
	
	public double getOutput(int taskID){
		double output = 0.0;
		for (int i = 0; i< rowNum; i++){
			output += (topOutputRateGraph.get(taskID, i)>0)?1:0;
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
	
	public int findDevByResourceAware(){
		// sort the nodes by the existing tasks in a descending order
		int[] sortedDevByExistingTasks =  sortedDevByExistingTasksInDecendingOrder(calUsageOfDevs());
		
		// find the first node with available executors, record it as start
		int startIndex = 0;
		for (int i=0;i<colNum;i++){
			int devID = sortedDevByExistingTasks[i];
			double availableExecutors = constraints[devID]-getDevUsage(devID);
			startIndex = i;
			if(availableExecutors>0){
				break;
			}
		}
		
		// find the last node with the same existing tasks as the start node, record it as end
		int startDevID = sortedDevByExistingTasks[startIndex];
		double existingExecutorsOfStart = getDevUsage(startDevID);
		int endIndex = startIndex;
		for(int j = startIndex; j<colNum; j++){
			int devID = sortedDevByExistingTasks[j];
			double existingExecutors = getDevUsage(devID);
			endIndex = j;
			if(existingExecutors<existingExecutorsOfStart){
				endIndex = j-1;
				break;
			}
		}
		
		// pick up the node with the most available executors from start to end
		int maxDevID = sortedDevByExistingTasks[startIndex];
		double maxAvailableExecutors = constraints[maxDevID]-getDevUsage(maxDevID);
		for (int k = startIndex; k<=endIndex; k++){
			int devID = sortedDevByExistingTasks[k];
			double availableExecutors = constraints[devID]-getDevUsage(devID);
			if(availableExecutors>maxAvailableExecutors){
				maxDevID = devID;
				maxAvailableExecutors = availableExecutors;
			}
		}

		return maxDevID;
	}
	
	public double getDevUsage(int devID){
		double usage = 0.0;
		for (int i=0; i< rowNum; i++){
			usage += bestSchedule.get(i, devID);
		}
		return usage;
	}
	
	public double[] calUsageOfDevs(){
		double[] existingTasks = new double[colNum];
		for (int i =0; i< colNum; i++){
			existingTasks[i] = getDevUsage(i);
		}
		return existingTasks;
	}
	
	public int[] sortedDevByExistingTasksInDecendingOrder (double[] existingTasks){
		int[] sortedDevs = new int[colNum];
		for(int i = 0; i<colNum; i++){
			int maxIndex = 0;
			double maxTasks = existingTasks[0];
			for (int j=0; j<colNum; j++){
				if(existingTasks[j]>maxTasks){
					maxIndex = j;
					maxTasks = existingTasks[j];
				}
			}
			sortedDevs[i] = maxIndex;
			existingTasks[maxIndex]=-1.0;
		}
		return sortedDevs;
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
