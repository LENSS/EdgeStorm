package nimbusscheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import utils.RandomNumGenerator;
import Jama.Matrix;

public class FBMStormAdvancedGeneticSearch {

	public static final int initialPopulationSize = 20;	
	public static final int parentMax = 10;				// parent size should not larger than 5
	public static final int parentRatio = 2;   			// 1/2 chosen as parents
	public static final int mutationMin = 1;			// mutation size should not smaller than 1
	public static final int mutationRatio = 5; 		// 1/5 chosen to mutate
	public static final int generation = 100;			// have 100 generations
	
	public static final double delayWeight = 0.5;
	public static final double energyWeight = 0.5;
	public static final double balanceWeight = 0;
	
	public double maxDelay;
	public double maxEnergy;
	
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
	
	public FBMStormAdvancedGeneticSearch(double[][] d2dPropDelay, double[][] d2dTransDelay, double[][] d2dEnergyPerbit,
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
	
	public FBMStormAdvancedGeneticSearch(double[][] d2dPropDelay, double[][] d2dTransDelay, double[][] d2dEnergyPerbit, 
			                     double[][] mt2tOutputRate, double[][] mt2tPktAvgSize){
		devPropDelayGraph = new Matrix(d2dPropDelay);
		devTransDelayGraph = new Matrix(d2dTransDelay);
		devEnergyPerbitGraph = new Matrix(d2dEnergyPerbit);
		topOutputRateGraph = new Matrix(mt2tOutputRate);
		topPktAvgSizeGraph = new Matrix(mt2tPktAvgSize);
	}
	
	public Matrix search(){
		FBMStormGeneticSearch gs1 = new FBMStormGeneticSearch();
		gs1.InitForMaxDelay(devPropDelayGraph, devTransDelayGraph, topOutputRateGraph, topPktAvgSizeGraph, constraints, FBMStormGeneticSearch.MAXDELAY);
		gs1.search();
		maxDelay = gs1.getBestMetric();
		
		System.out.println("maxDelay:"+maxDelay);
		
		FBMStormGeneticSearch gs2 = new FBMStormGeneticSearch();
		gs2.InitForMaxEnergy(devEnergyPerbitGraph, topOutputRateGraph, topPktAvgSizeGraph, constraints, FBMStormGeneticSearch.MAXENERGY);
		gs2.search();
		maxEnergy = gs2.getBestMetric();
		
		System.out.println("maxEnergy:"+maxEnergy);
		
		List<Matrix> population = initialSchedulePopulation(initialPopulationSize);
		bestSchedule = selectMinMetricSchedule(population);
		bestMetric = calculateMinMetric(bestSchedule);
		List<Matrix> parents = chooseParents(population,parentRatio);
		
		for(int i = 0; i < generation; i++){		
			List<Matrix> offSprings = generateOffspringSechedules(parents,i);
			
			List<Matrix> survialOffSprings = filterOffSpring(offSprings);
			if(survialOffSprings.size() == 0)
				break;			

			for(int j = 0; j < survialOffSprings.size(); j++){
				Matrix mutatedMatrix =mutate(survialOffSprings.get(j),mutationRatio);
				survialOffSprings.set(j, mutatedMatrix);
				if(i%2==0)
				{
					survialOffSprings.set(j,localImprovement(survialOffSprings.get(j)));
				}
			}
			
			Matrix curbest = selectMinMetricSchedule(survialOffSprings);
			double curbestMetric = calculateMinMetric(curbest);
			
			if(curbestMetric < bestMetric){
				bestSchedule = curbest;
				bestMetric = curbestMetric;
			}
						
			parents.addAll(survialOffSprings);
			parents = chooseParents(parents,parentRatio);
		}
		return bestSchedule;
	}
	
	public List<Matrix> chooseParents(List<Matrix> population, int parentRatio){	
		Collections.sort(population, new Comparator<Matrix>() {
	        @Override
	        public int compare(Matrix m1, Matrix m2)
	        {	
	        	Double m1TrafficMetric = calculateMinMetric(m1);
				Double m2TrafficMetric = calculateMinMetric(m2);
				return m1TrafficMetric.compareTo(m2TrafficMetric);
	        }
	    });

		int parentSize = (population.size()/parentRatio < parentMax)? population.size()/parentRatio:parentMax;
		
		// choose the best
		// List<Matrix> parents = population.subList(0, parentSize);
		
		// choose the random
		//List<Matrix> parents = new ArrayList<Matrix>();
		//for(int i = 0; i< parentSize; i++){
		//	parents.add(population.get(RandomNumGenerator.randomInt(population.size())));
		//}
		
		// choose with prob. according to metric
		List<Matrix> parents = new ArrayList<Matrix>();
		double[] metrics = new double[population.size()];
		double[] probSlot = new double[population.size()];
		double standard = 0.0;
		double bound = 0.0;
		for(int i = 0; i < population.size();i++){	
			metrics[i] = calculateMinMetric(population.get(i));
			standard = (metrics[0]==0.0)? 0.0001 : metrics[0];
			if(metrics[i]==0)
				probSlot[i] = standard/0.0001;
			else
				probSlot[i] = standard/metrics[i];
			bound += probSlot[i];
		}
		
		for(int i = 0;i < parentSize; i++){
			double randDouble = RandomNumGenerator.randomDouble(bound);
			int index = 0;
			for(index = 0; index< population.size(); index++){
				randDouble-=probSlot[index];
				if(randDouble<0)
					break;
			}
			if(index == population.size())
				index = index - 1;
			parents.add(population.get(index));
		}
		return parents;
	}
	
	public List<Matrix> generateOffspringSechedules(List<Matrix> parents, int gen){
		List<Matrix> offSprings = new ArrayList<Matrix>();
		int parentSize = parents.size();		
		for(int i=0;i<parentSize;i++)
			for(int j=i+1;j<parentSize;j++)
			{
				Matrix father = parents.get(i);
				Matrix mother = parents.get(j);
				List<Matrix> children = getOffspring(father, mother, gen);
				offSprings.addAll(children);
			}
		return offSprings;
	}

	public List<Matrix> getOffspring(Matrix father, Matrix mother, int gen){
		// Uniform crossover
/*		Matrix child1 = father.copy();
		Matrix child2 = mother.copy();
		int rowCh = gen%2;
		for(int i =0; i<rowNum; i++){
			if (i%2==rowCh){
				Matrix fatherRow = father.getMatrix(i, i, 0, colNum-1);
				Matrix motherRow = mother.getMatrix(i, i, 0, colNum-1);
				child1.setMatrix(i, i, 0, colNum-1, motherRow);
				child2.setMatrix(i, i, 0, colNum-1, fatherRow);
			}
		}
		List<Matrix> children = new ArrayList<Matrix>();
		children.add(child1);
		children.add(child2);
		return children;*/
		
		// Two-point crossover
		Matrix child1 = father.copy();
		Matrix child2 = mother.copy();
		int row1 = RandomNumGenerator.randomInt(rowNum);
		int row2 = RandomNumGenerator.randomInt(rowNum);
		int startRow = row1<row2?row1:row2;
		int endRow = row1>row2?row1:row2;
		Matrix fatherGen = father.getMatrix(startRow, endRow, 0, colNum-1);
		Matrix motherGen = mother.getMatrix(startRow, endRow, 0, colNum-1);
		child1.setMatrix(startRow, endRow, 0, colNum-1, motherGen);
		child2.setMatrix(startRow, endRow, 0, colNum-1, fatherGen);
		List<Matrix> children = new ArrayList<Matrix>();
		children.add(child1);
		children.add(child2);		
		return children;	
	}
	
	public List<Matrix> filterOffSpring(List<Matrix> offSprings){
		List<Matrix> survivalList = new ArrayList<Matrix>();
		int offSpringSize = offSprings.size();
		for(int i = 0; i< offSpringSize; i++){
			Matrix offspring = offSprings.get(i);
			if (judgeMatrixSatCons(offspring, constraints)){
				survivalList.add(offspring);
			}
		}
		return survivalList;
	}
	
	public boolean judgeMatrixSatCons(Matrix m, int[] constraint){
		for(int j=0; j<colNum;j++){
			double constraintJ = constraint[j];
			double colSumJ = 0.0;
			for(int i = 0;i<rowNum;i++){
				colSumJ += m.get(i, j);
			}
			if(colSumJ>constraintJ)
				return false;
		}
		return true;
	}
	
	public Matrix mutate(Matrix m, int mutateRate){
		// mutate by changing one line
/*		Matrix mutatedMatrix = m.copy();
  		int mutateRowNum = (m.getRowDimension()/mutateRate > mutationMin)?(m.getRowDimension()/mutateRate):mutationMin;	
		List<Integer> mutateRows = new ArrayList<Integer>();
		while(mutateRows.size() < mutateRowNum){
			int row = RandomNumGenerator.randomInt(rowNum);
			if (!mutateRows.contains(row)){
				mutateRows.add(row);
			}
		}
		for(int r:mutateRows){
			do{
				Matrix newRow = new Matrix(1,colNum);
				newRow.set(0, RandomNumGenerator.randomInt(colNum), 1);
				mutatedMatrix.setMatrix(r, r, 0, colNum-1, newRow);
			}while(!judgeMatrixSatCons(mutatedMatrix, constraints));
		}	 
		return mutatedMatrix;*/
		
		// mutate by changing two rows
		Matrix mutatedMatrix = m.copy();
		int mutateRowNum = (m.getRowDimension()/mutateRate > mutationMin)?(m.getRowDimension()/mutateRate):mutationMin;
		for(int i =0; i < mutateRowNum; i++){
			int rowNum1 = RandomNumGenerator.randomInt(rowNum);
			int rowNum2 = RandomNumGenerator.randomInt(rowNum);
			Matrix row1 = mutatedMatrix.getMatrix(rowNum1, rowNum1,0,colNum-1);
			Matrix row2 = mutatedMatrix.getMatrix(rowNum2, rowNum2,0,colNum-1);
			mutatedMatrix.setMatrix(rowNum2, rowNum2,0,colNum-1,row1);
			mutatedMatrix.setMatrix(rowNum1, rowNum1,0,colNum-1,row2);
		}
		return mutatedMatrix;
			
		// mutate by exchanging two columns
/*		Matrix mutatedMatrix = m.copy();
		int mutateRowNum = (m.getRowDimension()/mutateRate > mutationMin)?(m.getRowDimension()/mutateRate):mutationMin;
		for(int i =0; i < mutateRowNum; i++){	
			do{
				int colNum1 = RandomNumGenerator.randomInt(colNum);
				int colNum2 = RandomNumGenerator.randomInt(colNum);
				Matrix col1 = m.getMatrix(0, rowNum-1,colNum1,colNum1);
				Matrix col2 = m.getMatrix(0, rowNum-1,colNum2,colNum2);
				mutatedMatrix.setMatrix(0, rowNum-1,colNum2,colNum2, col1);
				mutatedMatrix.setMatrix(0, rowNum-1,colNum1,colNum1, col2);
			} while(!judgeMatrixSatCons(mutatedMatrix, constraints));
		}
		return mutatedMatrix;*/
	}
	
	public Matrix localImprovement(Matrix m){
		Matrix localBest = m;
		double localBestMetric = calculateMinMetric(m);
		
		int randomRowNum;
		for (randomRowNum = 0; randomRowNum<(m.getRowDimension()-1);randomRowNum++){
			Matrix rowCurrent = m.getMatrix(randomRowNum, randomRowNum, 0, colNum-1);
			
			Matrix neighbor = m.copy();
			Matrix rowNext = m.getMatrix(randomRowNum+1, randomRowNum+1, 0, colNum-1);
			neighbor.setMatrix(randomRowNum, randomRowNum, 0, colNum-1, rowNext);
			neighbor.setMatrix(randomRowNum+1, randomRowNum+1, 0, colNum-1, rowCurrent);
			if(judgeMatrixSatCons(neighbor, constraints)){		
				double neighborMetric = calculateMinMetric(neighbor);
				if(neighborMetric<localBestMetric){
					localBest = neighbor;
				}
			}
		}
		return localBest;
	}
	
	public List<Matrix> initialSchedulePopulation(int populationSize){
		List<Matrix> initialPopulation =  new ArrayList<Matrix>();
		while (initialPopulation.size()<populationSize){
			Matrix newMatrix = generateRandomMatrix();
			initialPopulation.add(newMatrix);
		}
		return initialPopulation;
	}
	
	public Matrix generateRandomMatrix(){
		Matrix newMatrix = new Matrix(rowNum,colNum);
		// totally random matrix
		double[] cons = new double[constraints.length];
		for(int i =0; i < constraints.length; i++){
			cons[i] = constraints[i];
		}
		
		for(int i = 0; i < rowNum; i++){
			int column;
			do{
				column = RandomNumGenerator.randomInt(colNum);
			} while(cons[column]<1.0);
			newMatrix.set(i,column, 1);
			cons[column] = cons[column] - 1 ;
		}
		//newMatrix.print(colNum, 0);
		return newMatrix;
	}
	
	public Matrix selectMinMetricSchedule(List<Matrix> population){
		double minMetric = Double.MAX_VALUE ;
		Matrix bestSchedule = null;
		for (Matrix m: population){
			double metric = calculateMinMetric(m);
			if (metric < minMetric){
				minMetric = metric;
				bestSchedule = m.copy(); 
			}	
		}
		return bestSchedule;
	}
	
	public double calculateMinMetric(Matrix allocation){
        double delayMetric = calculateDelay(allocation);
        double energyMetric = calculateEnergy(allocation);
        double balanceMetric = calculateBalanceIndex(allocation);
        double metric = delayWeight*delayMetric/maxDelay + energyWeight*energyMetric/maxEnergy + balanceWeight*balanceMetric;
		return metric;		
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
	
	public double calculateBalanceIndex(Matrix allocation){
		double totalBattery = 0.0;
		for(int i=0; i<colNum; i++){
			totalBattery += batteries[i];
		}
		
		double[] load = new double[colNum];
		double[][] allocArray = allocation.getArray();
		for(int i=0; i< colNum; i++){
			int totalTaskForOneDevice= 0;
			for(int j=0;j<rowNum;j++){
				totalTaskForOneDevice += allocArray[j][i];
			}
			if(totalTaskForOneDevice == 0){
				load[i] = (batteries[i]/totalBattery)/(0.1/rowNum);			// avoid the 0 case
			} else{
				load[i] = (batteries[i]/totalBattery)/(totalTaskForOneDevice*1.0/rowNum);
			}
		}
		
		double loadSum = 0.0;
		double loadSquareSum = 0.0;
		for(int i=0; i< colNum; i++){
			loadSum += load[i];
			loadSquareSum += load[i]*load[i];
		}
		
		double metric = 1-loadSum*loadSum/(colNum*loadSquareSum); 
		return metric;
	}
	
	public Matrix getBestSchedule(){
		return bestSchedule;
	}
	
	public double getBestMetric(){
		return bestMetric;
	}
}
