package nimbusscheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import utils.RandomNumGenerator;
import Jama.*;

public class FBMStormGeneticSearch {
	
	public static final int initialPopulationSize = 20;	
	public static final int parentMax = 10;				// parent size
	public static final int parentRatio = 2;   			// 1/2 chosen as parents
	public static final int mutationMin = 1;			// mutation size should not smaller than 1
	public static final int mutationRatio = 5; 		    // 1/5 chosen to mutate
	public static final int generation = 100;			// have 100 generations
	
	public static final int MINTRAFFIC = 0;
	public static final int MAXDELAY = 1;
	public static final int MAXENERGY= 2;
	
	Matrix t2tOutputRateGraph;
	Matrix t2tPacketSizeGraph;
	Matrix d2dDelayGraph;
	Matrix d2dPropDelayGraph;
	Matrix d2dTranDelayGraph;
	Matrix d2dEnergyPerBitGraph;	
	int[] constraints;	
	int rowNum = 0;
	int colNum = 0;
	int searchType;
	
    Matrix bestSchedule;
    double bestMetric;
	
	// For minimizing traffic size
	public void InitForMinTraffic (Matrix d2dCost, Matrix t2tTrafficRate, Matrix t2tTrafficSize, int[] exptExecutorsOfDevice, int type) {
		t2tOutputRateGraph = t2tTrafficRate.copy();
		t2tPacketSizeGraph = t2tTrafficSize.copy();
		d2dDelayGraph = d2dCost.copy();
		constraints = exptExecutorsOfDevice;
		searchType = type;
		rowNum = t2tOutputRateGraph.getRowDimension();
		colNum = d2dDelayGraph.getRowDimension();
	}
	
	// For maximizing delay
	public void InitForMaxDelay(Matrix d2dPropDelay, Matrix d2dTranDelay, Matrix t2tTrafficRate, Matrix t2tTrafficSize, int[] exptExecutorsOfDevice, int type){
		t2tOutputRateGraph = t2tTrafficRate.copy();
		t2tPacketSizeGraph = t2tTrafficSize.copy();
		d2dPropDelayGraph = d2dPropDelay.copy();
		d2dTranDelayGraph = d2dTranDelay.copy();
		constraints = exptExecutorsOfDevice;
		searchType = type;
		rowNum = t2tOutputRateGraph.getRowDimension();
		colNum = d2dPropDelayGraph.getRowDimension();
	}
	
	// For maximizing energy
	public void InitForMaxEnergy(Matrix d2dEnergy, Matrix t2tTrafficRate, Matrix t2tTrafficSize, int[] exptExecutorsOfDevice, int type){
		t2tOutputRateGraph = t2tTrafficRate.copy();
		t2tPacketSizeGraph = t2tTrafficSize.copy();
		d2dEnergyPerBitGraph = d2dEnergy.copy();
		constraints = exptExecutorsOfDevice;
		searchType = type;
		rowNum = t2tOutputRateGraph.getRowDimension();
		colNum = d2dEnergyPerBitGraph.getRowDimension();
	}
	
	public Matrix search(){
		List<Matrix> population = initialSchedulePopulation(initialPopulationSize);
		
        switch (searchType) {
        	case MINTRAFFIC:  	bestSchedule = selectMinTrafficMetricSchedule(population);
        						bestMetric = calculateMinTrafficMetric(bestSchedule);
                 				break;
        	case MAXDELAY:  	bestSchedule = selectMaxDelayMetricSchedule(population);
        						bestMetric = calculateMaxDelayMetric(bestSchedule);
                 				break;
        	case MAXENERGY:     bestSchedule = selectMaxEnergyMetricSchedule(population);
        						bestMetric = calculateMaxEnergyMetric(bestSchedule);
                 				break;
        	default: 			bestSchedule = null;
        						bestMetric = -1;
        						break;
        }
		
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
			Matrix curbest;
			double curbestMetric;
	        switch (searchType) {
	        	case MINTRAFFIC:  	curbest = selectMinTrafficMetricSchedule(survialOffSprings);
	        						curbestMetric = calculateMinTrafficMetric(curbest);
									if(curbestMetric < bestMetric){
										bestSchedule = curbest;
										bestMetric = curbestMetric;
									}
	                 				break;
	        	case MAXDELAY:  	curbest = selectMaxDelayMetricSchedule(survialOffSprings);
	        						curbestMetric = calculateMaxDelayMetric(curbest);
									if(curbestMetric > bestMetric){
										bestSchedule = curbest;
										bestMetric = curbestMetric;
									}
	                 				break;
	        	case MAXENERGY:     curbest = selectMaxEnergyMetricSchedule(survialOffSprings);
	        						curbestMetric = calculateMaxEnergyMetric(curbest);
									if(curbestMetric > bestMetric){
										bestSchedule = curbest;
										bestMetric = curbestMetric;
									}
	                 				break;
	        	default: 			curbest = null;
	        						curbestMetric = -1;
	        						break;
	        }				
			parents.addAll(survialOffSprings);
			parents = chooseParents(parents,parentRatio);
		}
		return bestSchedule;
	}
	
	public List<Matrix> initialSchedulePopulation(int populationSize){
		List<Matrix> initialPopulation =  new ArrayList<Matrix>();
		while (initialPopulation.size()<populationSize){
			Matrix newMatrix = generateRandomMatrix();
			initialPopulation.add(newMatrix);
		}
		return initialPopulation;
	}
	
	public Matrix selectMinTrafficMetricSchedule(List<Matrix> population){
		double minMetric = Double.MAX_VALUE ;
		Matrix bestSchedule = null;
		for (Matrix m: population){
			double metric = calculateMinTrafficMetric(m);
			if (metric < minMetric){
				minMetric = metric;
				bestSchedule = m.copy(); 
			}	
		}
		return bestSchedule;
	}
	
	public Matrix selectMaxDelayMetricSchedule(List<Matrix> population){
		double maxMetric = 0;
		Matrix bestSchedule = null;
		for (Matrix m: population){
			double metric = calculateMaxDelayMetric(m);
			if (metric > maxMetric){
				maxMetric = metric;
				bestSchedule = m.copy(); 
			}	
		}
		return bestSchedule;
	}
	
	public Matrix selectMaxEnergyMetricSchedule(List<Matrix> population){
		double maxMetric = 0;
		Matrix bestSchedule = null;
		for (Matrix m: population){
			double metric = calculateMaxEnergyMetric(m);
			if (metric > maxMetric){
				maxMetric = metric;
				bestSchedule = m.copy(); 
			}	
		}
		return bestSchedule;
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
		double localBestMetric;
        switch (searchType) {
	    	case MINTRAFFIC:  	localBestMetric = calculateMinTrafficMetric(m);
	             				break;
	    	case MAXDELAY:  	localBestMetric = calculateMaxDelayMetric(m);
	             				break;
	    	case MAXENERGY:     localBestMetric = calculateMaxEnergyMetric(m);
	             				break;
	    	default: 			localBestMetric = Double.MAX_VALUE; 
	    						break;
        }
		int randomRowNum;
		for (randomRowNum = 0; randomRowNum<(m.getRowDimension()-1);randomRowNum++){
			Matrix rowCurrent = m.getMatrix(randomRowNum, randomRowNum, 0, colNum-1);
			
			Matrix neighbor = m.copy();
			Matrix rowNext = m.getMatrix(randomRowNum+1, randomRowNum+1, 0, colNum-1);
			neighbor.setMatrix(randomRowNum, randomRowNum, 0, colNum-1, rowNext);
			neighbor.setMatrix(randomRowNum+1, randomRowNum+1, 0, colNum-1, rowCurrent);
			if(judgeMatrixSatCons(neighbor, constraints)){		
				double neighborMetric = 0.0;
		        switch (searchType) {
			    	case MINTRAFFIC:  	neighborMetric = calculateMinTrafficMetric(neighbor);
			             				break;
			    	case MAXDELAY:  	neighborMetric = calculateMaxDelayMetric(neighbor);
			             				break;
			    	case MAXENERGY:     neighborMetric = calculateMaxEnergyMetric(neighbor);
			             				break;
			    	default: 			neighborMetric = Double.MAX_VALUE; 
			    						break;
		        }
				if(neighborMetric<localBestMetric){
					localBest = neighbor;
				}
			}
		}
		return localBest;
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
	
	public Matrix generateRandomMatrix(){
		Matrix newMatrix = new Matrix(rowNum,colNum);
	
		int[] cons = new int[constraints.length];
		for(int i = 0; i < constraints.length; i++){
			cons[i] = constraints[i];
		}
		
		for(int i = 0; i < rowNum; i++){
			int column = RandomNumGenerator.randomInt(colNum);
			while(cons[column] == 0){
				column = (++column)%colNum;
			}
			newMatrix.set(i,column, 1);
			cons[column] = cons[column] - 1 ;
		}
		//newMatrix.print(colNum, 0);
		return newMatrix;
	}
	
	public List<Matrix> chooseParents(List<Matrix> population, int parentRatio){	
		Collections.sort(population, new Comparator<Matrix>() {
	        @Override
	        public int compare(Matrix m1, Matrix m2)
	        {	        	
	        	switch (searchType) {
	        		case MINTRAFFIC:  	Double m1TrafficMetric = calculateMinTrafficMetric(m1);
		            					Double m2TrafficMetric = calculateMinTrafficMetric(m2);
		            					return m1TrafficMetric.compareTo(m2TrafficMetric);
	        		case MAXDELAY:  	Double m1DelayMetric = calculateMaxDelayMetric(m1);
										Double m2DelayMetric = calculateMaxDelayMetric(m2);
										return m2DelayMetric.compareTo(m1DelayMetric);
	        		case MAXENERGY:     Double m1EnergyMetric = calculateMaxEnergyMetric(m1);
										Double m2EnergyMetric = calculateMaxEnergyMetric(m2);
										return m2EnergyMetric.compareTo(m1EnergyMetric);
					default: return 0; 
	        	}
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
			switch (searchType) {
				case MINTRAFFIC: 	metrics[i] = calculateMinTrafficMetric(population.get(i));
									standard = (metrics[0]==0.0)? 0.0001 : metrics[0];
									if(metrics[i]==0)
										probSlot[i] = standard/0.0001;
									else
										probSlot[i] = standard/metrics[i];
									bound += probSlot[i];
									break;
				case MAXDELAY: 		metrics[i] = calculateMaxDelayMetric(population.get(i));
									standard = metrics[0];
									probSlot[i] = metrics[i]/standard;
									bound += probSlot[i];
									break;
				case MAXENERGY:     metrics[i] = calculateMaxEnergyMetric(population.get(i));
									standard = metrics[0];
									probSlot[i] = metrics[i]/standard;
									bound += probSlot[i];
									break;
			}
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

	public double calculateMinTrafficMetric(Matrix allocation){
		double metric = 0.0;
		Matrix trafficSizeMatrix = t2tOutputRateGraph.arrayTimes(t2tPacketSizeGraph);
		Matrix dataMatrix = trafficSizeMatrix.times(allocation);
		Matrix commMatrix = allocation.times(d2dDelayGraph);
		Matrix costMatrix = dataMatrix.arrayTimes(commMatrix);
		double [][] matrix = costMatrix.getArray();
		int matrixRow = costMatrix.getRowDimension();
		int matrixCol = costMatrix.getColumnDimension();
		for (int i = 0; i < matrixRow; i++)
	    {
	        for (int j = 0; j < matrixCol; j++)
	        {
	        	metric += matrix[i][j];
	        }
	    }
		return metric;		
	}
	
	public double calculateMaxDelayMetric(Matrix allocation){
		double metric = 0.0;
		
		Matrix dataMatrix1 = t2tOutputRateGraph.times(allocation);
		Matrix commMatrix1 = allocation.times(d2dPropDelayGraph);
		Matrix costMatrix1 = dataMatrix1.arrayTimes(commMatrix1);
		
		Matrix trafficSizeMatrix = t2tOutputRateGraph.arrayTimes(t2tPacketSizeGraph);
		Matrix dataMatrix2 = trafficSizeMatrix.times(allocation);
		Matrix commMatrix2 = allocation.times(d2dTranDelayGraph);
		Matrix costMatrix2 = dataMatrix2.arrayTimes(commMatrix2);
		
		Matrix costMatrix = costMatrix1.plus(costMatrix2);
		
		double [][] matrix = costMatrix.getArray();
		int matrixRow = costMatrix.getRowDimension();
		int matrixCol = costMatrix.getColumnDimension();
		for (int i = 0; i < matrixRow; i++)
	    {
	        for (int j = 0; j < matrixCol; j++)
	        {
	        	metric += matrix[i][j];
	        }
	    }
		return metric;	
	}
	
	public double calculateMaxEnergyMetric(Matrix allocation){
		double metric = 0.0;
		Matrix trafficSizeMatrix = t2tOutputRateGraph.arrayTimes(t2tPacketSizeGraph);
		Matrix dataMatrix = trafficSizeMatrix.times(allocation);
		Matrix energyMatrix = allocation.times(d2dEnergyPerBitGraph);
		Matrix costMatrix = dataMatrix.arrayTimes(energyMatrix);
		double [][] matrix = costMatrix.getArray();
		int matrixRow = costMatrix.getRowDimension();
		int matrixCol = costMatrix.getColumnDimension();
		for (int i = 0; i < matrixRow; i++)
	    {
	        for (int j = 0; j < matrixCol; j++)
	        {
	        	metric += matrix[i][j];
	        }
	    }
		return metric;		
	}
	
	public double getBestMetric(){
		return bestMetric;
	}
	
	public Matrix getBestSchedule(){
		return bestSchedule;
	}
}
