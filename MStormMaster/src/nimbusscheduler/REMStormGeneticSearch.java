package nimbusscheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import Jama.*;
import utils.RandomNumGenerator;

public class REMStormGeneticSearch {
	// parameters for genetic algorithm
	public static final int initialPopulationSize = 20;	
	public static final int parentMax = 10;				// maximum parent size
	public static final int parentRatio = 2;   			// 1/2 chosen as parents
	public static final int mutationMin = 1;			// minimum mutation size
	public static final int mutationRatio = 5; 		    // 1/5 chosen to mutate
	public static final int generation = 300;			// have 100 generations
	
	public static final int MINAVAIL = 0;
	public static final int MAXAVAIL = 1;
	public static final int MINDIVERSITY = 2;
	public static final int MAXDIVERSITY = 3;
	public static final int MINTRAFFIC = 4;
	public static final int MAXTRAFFIC = 5;
	public static final int MAXTRARESIL = 6;
	
	// input parameters for each func
	Matrix t2tTraffic;
	int[] parallelism;
	int[] devCap;
	double[] devAvail;
	int compNum;
	int taskNum;
	int devNum;
	int searchType;
	
	// input parameters for multiObjFunc
	double availWeight;
	double diversityWeight;
	double trafficWeight;
	double maxAvail;
	double minAvail;
	double maxDiversity;
	double minDiversity;
	double maxTraffic;
	double minTraffic;
	
	//output parameters
    Matrix bestSchedule;
    double bestMetric;
    
    public REMStormGeneticSearch(Matrix t2tTraffic, int[] parallelism, int[] devCap, double[] devAvail, 
    					 int compNum, int taskNum, int devNum, int searchType) {
    	this.t2tTraffic = t2tTraffic;
    	this.parallelism = parallelism;
    	this.devCap = devCap;
    	this.devAvail = devAvail;
    	this.compNum = compNum;
    	this.taskNum = taskNum;
    	this.devNum = devNum;
    	this.searchType = searchType;
    }
    
    public REMStormGeneticSearch(Matrix t2tTraffic, int[] parallelism, int[] devCap, double[] devAvail, 
    					int compNum, int taskNum, int devNum, int searchType, double availWeight, 
    					double diversityWeight, double trafficWeight, double maxAvail, double minAvail,
    					double maxDiversity, double minDiversity, double maxTraffic, double minTraffic) {
    	this.t2tTraffic = t2tTraffic;
    	this.parallelism = parallelism;
    	this.devCap = devCap;
    	this.devAvail = devAvail;
    	this.compNum = compNum;
    	this.taskNum = taskNum;
    	this.devNum = devNum;
    	this.searchType = searchType;
    	this.availWeight = availWeight;
    	this.diversityWeight = diversityWeight;
    	this.trafficWeight = trafficWeight;
    	this.maxAvail = maxAvail;
    	this.minAvail = minAvail;
    	this.maxDiversity = maxDiversity;
    	this.minDiversity = minDiversity;
    	this.maxTraffic = maxTraffic;
    	this.minTraffic = minTraffic;
    }
	
	public Matrix search(){
		List<Matrix> population = initialSchedulePopulation(initialPopulationSize);
		
        switch (searchType) {
		    case MINAVAIL:		bestSchedule = selectAvailMetricSchedule(population, MINAVAIL);
		    					bestMetric = calculateAvailMetric(bestSchedule);
		    					break;
		    case MAXAVAIL:		bestSchedule = selectAvailMetricSchedule(population, MAXAVAIL);
								bestMetric = calculateAvailMetric(bestSchedule);
								break;
		    case MINDIVERSITY:  bestSchedule = selectDiversityMetricSchedule(population, MINDIVERSITY);
								bestMetric = calculateDiversityMetric(bestSchedule);
								break;
		    case MAXDIVERSITY:  bestSchedule = selectDiversityMetricSchedule(population, MAXDIVERSITY);
								bestMetric = calculateDiversityMetric(bestSchedule);
								break;
		    case MINTRAFFIC:    bestSchedule = selectTrafficMetricSchedule(population, MINTRAFFIC);
								bestMetric = calculateTrafficMetric(bestSchedule);
								break;
		    case MAXTRAFFIC:    bestSchedule = selectTrafficMetricSchedule(population, MAXTRAFFIC);
								bestMetric = calculateTrafficMetric(bestSchedule);
								break;
		    case MAXTRARESIL:	bestSchedule = selectTraResilMetricSchedule(population);
							    bestMetric = calculateTraResilMetric(bestSchedule);
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
	        	case MINDIVERSITY:  curbest = selectDiversityMetricSchedule(survialOffSprings, MINDIVERSITY);
	        						curbestMetric = calculateDiversityMetric(curbest);
									if(curbestMetric < bestMetric){
										bestMetric = curbestMetric;
										bestSchedule = curbest;
									}
	                 				break;
	        	case MAXDIVERSITY:  curbest = selectDiversityMetricSchedule(survialOffSprings, MAXDIVERSITY);
	        						curbestMetric = calculateDiversityMetric(curbest);
									if(curbestMetric > bestMetric){
										bestMetric = curbestMetric;
										bestSchedule = curbest;
									}
	                 				break;
	        	case MINAVAIL: 	 	curbest = selectAvailMetricSchedule(survialOffSprings, MINAVAIL);
									curbestMetric = calculateAvailMetric(curbest);
									if(curbestMetric < bestMetric){
										bestMetric = curbestMetric;
										bestSchedule = curbest;
									}
					 				break;
	        	case MAXAVAIL:  	curbest = selectAvailMetricSchedule(survialOffSprings, MAXAVAIL);
									curbestMetric = calculateAvailMetric(curbest);
									if(curbestMetric > bestMetric){
										bestMetric = curbestMetric;
										bestSchedule = curbest;
									}
					 				break;
	        	case MINTRAFFIC:  	curbest = selectTrafficMetricSchedule(survialOffSprings, MINTRAFFIC);
									curbestMetric = calculateTrafficMetric(curbest);
									if(curbestMetric < bestMetric){
										bestMetric = curbestMetric;
										bestSchedule = curbest;
									}
					 				break;
				case MAXTRAFFIC:  	curbest = selectTrafficMetricSchedule(survialOffSprings, MAXTRAFFIC);
									curbestMetric = calculateTrafficMetric(curbest);
									if(curbestMetric > bestMetric){
										bestMetric = curbestMetric;
										bestSchedule = curbest;
									}
									break;
				case MAXTRARESIL:  	curbest = selectTraResilMetricSchedule(survialOffSprings);
									curbestMetric = calculateTraResilMetric(curbest);
									if(curbestMetric > bestMetric){
										bestMetric = curbestMetric;
										bestSchedule = curbest;
									}
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
	
	public Matrix selectAvailMetricSchedule(List<Matrix> population, int type){
		Matrix bestSchedule = null;
		if(type == MINAVAIL) {
			double minMetric = Double.MAX_VALUE ;
			for (Matrix m: population){
				double metric = calculateAvailMetric(m);
				if (metric < minMetric){
					minMetric = metric;
					bestSchedule = m.copy(); 
				}	
			}
		} else {
			double maxMetric = -1 ;
			for (Matrix m: population){
				double metric = calculateAvailMetric(m);
				if (metric > maxMetric){
					maxMetric = metric;
					bestSchedule = m.copy(); 
				}	
			}
		}
		return bestSchedule;
	}
		
	public Matrix selectDiversityMetricSchedule(List<Matrix> population, int type){
		Matrix bestSchedule = null;
		if(type == MINDIVERSITY) {
			double minMetric = Double.MAX_VALUE ;
			for (Matrix m: population){
				double metric = calculateDiversityMetric(m);
				if (metric < minMetric){
					minMetric = metric;
					bestSchedule = m.copy(); 
				}	
			}
		} else {
			double maxMetric = -1 ;
			for (Matrix m: population){
				double metric = calculateDiversityMetric(m);
				if (metric > maxMetric){
					maxMetric = metric;
					bestSchedule = m.copy(); 
				}	
			}
		}
		return bestSchedule;
	}
	
	public Matrix selectTrafficMetricSchedule(List<Matrix> population, int type){
		Matrix bestSchedule = null;
		if(type == MINTRAFFIC) {
			double minMetric = Double.MAX_VALUE ;
			for (Matrix m: population){
				double metric = calculateTrafficMetric(m);
				if (metric < minMetric){
					minMetric = metric;
					bestSchedule = m.copy(); 
				}	
			}
		} else {
			double maxMetric = -1 ;
			for (Matrix m: population){
				double metric = calculateTrafficMetric(m);
				if (metric > maxMetric){
					maxMetric = metric;
					bestSchedule = m.copy(); 
				}	
			}
		}
		return bestSchedule;
	}
		
	public Matrix selectTraResilMetricSchedule(List<Matrix> population) {
		Matrix bestSchedule = null;
		double maxMetric = -1 ;
		for (Matrix m: population){
			double metric = calculateTraResilMetric(m);
			if (metric > maxMetric){
				maxMetric = metric;
				bestSchedule = m.copy(); 
			}	
		}
		return bestSchedule;
	}
	
	public double calculateAvailMetric(Matrix allocation){
		double metric = 1.0;
		for (int k = 0; k < devNum; k++) {
			double usageK = 1.0;
			for(int j = 0; j < taskNum; j++) {
				usageK *= (1-allocation.get(j, k));
			}
			usageK = 1 - usageK;
			metric = metric * Math.pow(devAvail[k], usageK);
		}
		return metric;		
	}
	
	public double calculateDiversityMetric(Matrix allocation){
		double metric = 1.0;
		// calculate compAssign
		double[][] compAssign = new double[compNum][devNum];
		int startIndexOfComponentI = 0;
		for(int i = 0; i < compNum; i++) {
			for (int k = 0; k < devNum; k++) {
				double compi2Devk = 1.0;
				for (int j = startIndexOfComponentI; j < (startIndexOfComponentI + parallelism[i]); j++) {
					compi2Devk *= (1-allocation.get(j, k));
				}
				compi2Devk = 1 - compi2Devk;
				compAssign[i][k] = compi2Devk;
			}
			startIndexOfComponentI += parallelism[i]; 
		}
		
		// calculate the metric
		for (int i = 0; i < compNum; i++) {
			double diversityOfCompI = 0;
			for (int k = 0; k < devNum; k++) {
				diversityOfCompI += compAssign[i][k];
			}
			metric *= diversityOfCompI;
		}
		return metric;	
	}
	
	public double calculateTrafficMetric(Matrix allocation){
		double metric = 0.0;
		for(int j = 0; j < taskNum; j++) {
			for(int jplus = 0; jplus < taskNum; jplus++) {
				double trafficJJplus = t2tTraffic.get(j, jplus);
				double d2dJJplus = 0.0;
				for (int k = 0; k < devNum; k++) {
					d2dJJplus += allocation.get(j, k) * allocation.get(jplus, k);
				}
				metric += trafficJJplus * (1-d2dJJplus);
			}
		}
		return metric;		
	}
	
	public double calculateTraResilMetric(Matrix allocation) {
		double availMetric = calculateAvailMetric(allocation);
		double diversityMetric = calculateDiversityMetric(allocation);
		double trafficMetric = calculateTrafficMetric(allocation);
		double availPart = (maxAvail==minAvail) ? 1.0 : (availMetric - minAvail)/(maxAvail - minAvail);
		double diversityPart = (maxDiversity==minDiversity) ? 1.0 : (diversityMetric - minDiversity)/(maxDiversity - minDiversity);
		double trafficPart = (maxTraffic==minTraffic)? 1.0 : (maxTraffic - trafficMetric)/(maxTraffic - minTraffic);	
		double traResilMetric = availWeight * availPart + diversityWeight * diversityPart + trafficWeight * trafficPart;
		return traResilMetric;
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
		int row1 = RandomNumGenerator.randomInt(taskNum);
		int row2 = RandomNumGenerator.randomInt(taskNum);
		int startRow = row1 < row2 ? row1 : row2;
		int endRow = row1 > row2 ? row1 : row2;
		Matrix fatherGen = father.getMatrix(startRow, endRow, 0, devNum-1);
		Matrix motherGen = mother.getMatrix(startRow, endRow, 0, devNum-1);
		child1.setMatrix(startRow, endRow, 0, devNum-1, motherGen);
		child2.setMatrix(startRow, endRow, 0, devNum-1, fatherGen);
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
			int rowNum1 = RandomNumGenerator.randomInt(taskNum);
			int rowNum2 = RandomNumGenerator.randomInt(taskNum);
			Matrix row1 = mutatedMatrix.getMatrix(rowNum1, rowNum1,0,devNum-1);
			Matrix row2 = mutatedMatrix.getMatrix(rowNum2, rowNum2,0,devNum-1);
			mutatedMatrix.setMatrix(rowNum2, rowNum2,0,devNum-1,row1);
			mutatedMatrix.setMatrix(rowNum1, rowNum1,0,devNum-1,row2);
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
		
		double localBestMetric = 0.0;
        switch (searchType) {
    		case MINAVAIL:  	
    		case MAXAVAIL: 		localBestMetric = calculateAvailMetric(m);
								break;
    		case MINDIVERSITY:  
    		case MAXDIVERSITY:	localBestMetric = calculateDiversityMetric(m);
								break;
	    	case MINTRAFFIC:  	
	    	case MAXTRAFFIC:	localBestMetric = calculateTrafficMetric(m);
	             				break;
        }
        
		int randomRowNum;
		for (randomRowNum = 0; randomRowNum<(m.getRowDimension()-1);randomRowNum++){
			Matrix rowCurrent = m.getMatrix(randomRowNum, randomRowNum, 0, devNum-1);
			Matrix rowNext = m.getMatrix(randomRowNum+1, randomRowNum+1, 0, devNum-1);
			Matrix neighbor = m.copy();
			neighbor.setMatrix(randomRowNum, randomRowNum, 0, devNum-1, rowNext);
			neighbor.setMatrix(randomRowNum+1, randomRowNum+1, 0, devNum-1, rowCurrent);
			if(judgeMatrixSatCons(neighbor, devCap)){		
				double neighborMetric = 0.0;
		        switch (searchType) {
			    	case MINAVAIL:     	neighborMetric = calculateAvailMetric(neighbor);
								    	if(neighborMetric<localBestMetric){
											localBestMetric = neighborMetric;
											localBest = neighbor;
										}
										break;
			    	case MAXAVAIL:		neighborMetric = calculateAvailMetric(neighbor);
								    	if(neighborMetric>localBestMetric){
											localBestMetric = neighborMetric;
											localBest = neighbor;
										}
										break;
			    	case MINTRAFFIC:  	neighborMetric = calculateTrafficMetric(neighbor);
			    						if(neighborMetric<localBestMetric){
			    							localBestMetric = neighborMetric;
			    							localBest = neighbor;
			    						}
     									break;
			    	case MAXTRAFFIC: 	neighborMetric = calculateTrafficMetric(neighbor);
								    	if(neighborMetric>localBestMetric){
											localBestMetric = neighborMetric;
											localBest = neighbor;
										}     				
								    	break;
			    	case MINDIVERSITY:  neighborMetric = calculateDiversityMetric(neighbor);
								    	if(neighborMetric<localBestMetric){
											localBestMetric = neighborMetric;
											localBest = neighbor;
										}					
								    	break;	
			    	case MAXDIVERSITY:	neighborMetric = calculateDiversityMetric(neighbor);
								    	if(neighborMetric>localBestMetric){
											localBestMetric = neighborMetric;
											localBest = neighbor;
										}
			    						break;
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
			if (judgeMatrixSatCons(offspring, devCap)){
				survivalList.add(offspring);
			}
		}
		return survivalList;
	}
	
	public boolean judgeMatrixSatCons(Matrix m, int[] constraint){
		for(int j=0; j<devNum;j++){
			double constraintJ = constraint[j];
			double colSumJ = 0.0;
			for(int i = 0;i<taskNum;i++){
				colSumJ += m.get(i, j);
			}
			if(colSumJ>constraintJ)
				return false;
		}
		return true;
	}
	
	public Matrix generateRandomMatrix(){
		Matrix newMatrix = new Matrix(taskNum,devNum);
		int[] cons = devCap.clone();
		for(int i = 0; i < taskNum; i++){
			int column = RandomNumGenerator.randomInt(devNum);
			while(cons[column] == 0){
				column = (++column)%devNum;
			}
			newMatrix.set(i,column, 1);
			cons[column] = cons[column] - 1 ;
		}
		return newMatrix;
	}
	
	public List<Matrix> chooseParents(List<Matrix> population, int parentRatio){	
		Collections.sort(population, new Comparator<Matrix>() {
	        @Override
	        public int compare(Matrix m1, Matrix m2)
	        {	        	
	        	switch (searchType) {
	        		case MINAVAIL:     	Double m1MinAvailMetric = calculateAvailMetric(m1);
										Double m2MinAvailMetric = calculateAvailMetric(m2);
										return m1MinAvailMetric.compareTo(m2MinAvailMetric);
	        		case MAXAVAIL:  	Double m1MaxAvailMetric = calculateAvailMetric(m1);
										Double m2MaxAvailMetric = calculateAvailMetric(m2);
										return m2MaxAvailMetric.compareTo(m1MaxAvailMetric);
					case MINDIVERSITY:  Double m1MinDiversityMetric = calculateDiversityMetric(m1);
										Double m2MinDiversityMetric = calculateDiversityMetric(m2);
										return m1MinDiversityMetric.compareTo(m2MinDiversityMetric);
					case MAXDIVERSITY:  Double m1MaxDiversityMetric = calculateDiversityMetric(m1);
										Double m2MaxDiversityMetric = calculateDiversityMetric(m2);
										return m2MaxDiversityMetric.compareTo(m1MaxDiversityMetric);
	        		case MINTRAFFIC:  	Double m1MinTrafficMetric = calculateTrafficMetric(m1);
		            					Double m2MinTrafficMetric = calculateTrafficMetric(m2);
		            					return m1MinTrafficMetric.compareTo(m2MinTrafficMetric);
	        		case MAXTRAFFIC:  	Double m1MaxTrafficMetric = calculateTrafficMetric(m1);
										Double m2MaxTrafficMetric = calculateTrafficMetric(m2);
										return m2MaxTrafficMetric.compareTo(m1MaxTrafficMetric);
					default: 			return 0; 
	        	}
	        }
	    });

		int parentSize = (population.size()/parentRatio < parentMax) ? population.size()/parentRatio : parentMax;
		
		// choose the best parents
		// List<Matrix> parents = population.subList(0, parentSize);
		
		// choose the random parents
		List<Matrix> parents = new ArrayList<Matrix>();
		for(int i = 0; i< parentSize; i++){
			parents.add(population.get(RandomNumGenerator.randomInt(population.size())));
		}
		
		// choose parents with prob. according to metric
//		List<Matrix> parents = new ArrayList<Matrix>();
//		double[] metrics = new double[population.size()];
//		double[] probSlot = new double[population.size()];
//		double standard = 0.0;
//		double bound = 0.0;
//		for(int i = 0; i < population.size();i++){
//			switch (searchType) {
//				case MINAVAIL: 		metrics[i] = calculateAvailMetric(population.get(i));
//									standard = (metrics[0]==0.0)? 0.0001 : metrics[0];
//									if(metrics[i]==0)
//										probSlot[i] = standard/0.0001;
//									else
//										probSlot[i] = standard/metrics[i];
//									bound += probSlot[i];
//									break;
//				case MAXAVAIL: 		metrics[i] = calculateAvailMetric(population.get(i));
//									standard = (metrics[0]==0.0)? 0.0001 : metrics[0];
//									if(metrics[i]==0)
//										probSlot[i] = 0.0001/standard;
//									else
//										probSlot[i] = metrics[i]/standard;
//									bound += probSlot[i];
//									break;
//				case MINDIVERSITY: 	metrics[i] = calculateDiversityMetric(population.get(i));
//									standard = (metrics[0]==0.0)? 0.0001 : metrics[0];
//									if(metrics[i]==0)
//										probSlot[i] = standard/0.0001;
//									else
//										probSlot[i] = standard/metrics[i];
//									bound += probSlot[i];
//									break;
//				case MAXDIVERSITY: 	metrics[i] = calculateDiversityMetric(population.get(i));
//									standard = (metrics[0]==0.0)? 0.0001 : metrics[0];
//									if(metrics[i]==0)
//										probSlot[i] = 0.0001/standard;
//									else
//										probSlot[i] = metrics[i]/standard;
//									bound += probSlot[i];
//									break;
//				case MINTRAFFIC: 	metrics[i] = calculateTrafficMetric(population.get(i));
//									standard = (metrics[0]==0.0)? 0.0001 : metrics[0];
//									if(metrics[i]==0)
//										probSlot[i] = standard/0.0001;
//									else
//										probSlot[i] = standard/metrics[i];
//									bound += probSlot[i];
//									break;
//				case MAXTRAFFIC: 	metrics[i] = calculateTrafficMetric(population.get(i));
//									standard = (metrics[0]==0.0)? 0.0001 : metrics[0];
//									if(metrics[i]==0)
//										probSlot[i] = 0.0001/standard;
//									else
//										probSlot[i] = metrics[i]/standard;
//									bound += probSlot[i];
//									break;
//			}
//		}
//		for(int i = 0;i < parentSize; i++){
//			double randDouble = RandomNumGenerator.randomDouble(bound);
//			int index = 0;
//			for(index = 0; index< population.size(); index++){
//				randDouble-=probSlot[index];
//				if(randDouble<0)
//					break;
//			}
//			// just in case
//			if(index == population.size())
//				index = index - 1;
//			parents.add(population.get(index));
//		}
		
		return parents;
	}

	public double getBestMetric(){
		return bestMetric;
	}
	
	public Matrix getBestSchedule(){
		return bestSchedule;
	}
}
