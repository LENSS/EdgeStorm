package nimbusscheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import Jama.Matrix;
import amvp.Model;
import amvp.ModelCatlog;
import amvp.SplittedModel;
import utils.RandomNumGenerator;

public class AMVPGeneticSearch {	
	// parameters for genetic algorithm
	public static final int initialPopulationSize = 8;	
	public static final int parentMax = 6;				// maximum parent size
	public static final int parentRatio = 2;   			// 1/2 chosen as parents
	public static final int mutationMin = 1;			// minimum mutation size
	public static final int mutationRatio = 5; 		    // 1/5 chosen to mutate
	public static final int generation = 100;			// have 100 generations
	
	// input parameters
	double[] accReq;  // in gender, emotion, age order
	double[] lanReq;  // unit: ms
	double[] thrReq;  // unit: F/s
	double[] alpha;
	double[] belta;
	double[] gamma;
	double[] cpuRes; // range: (0,1]
	double[] memRes;
	double[] netBW;  // unit: Mbps
	List<SplittedModel> splittedModels; // common, gender, emotion, age
	int taskNum; // 4
	int devNum;
	
	//output parameters
    Matrix bestSchedule;
    double bestMetric;
        
    // parameter for early exit
    boolean earlyExit;
    
    double gender_acc_metric;
    double gender_lan_metric;
    double gender_thr_metric;
    double emotion_acc_metric;
    double emotion_lan_metric;
    double emotion_thr_metric;
    double age_acc_metric;
    double age_lan_metric;
    double age_thr_metric;
    
    double gender_acc;
    double gender_lan;
    double gender_thr;
    double emotion_acc;
    double emotion_lan;
    double emotion_thr;
    double age_acc;
    double age_lan;
    double age_thr;
    
    double gender_metric;
    double emotion_metric;
    double age_metric;
	
	public AMVPGeneticSearch (double[] accReq, double[] lanReq, double[] thrReq, 
							  double[] alpha,  double[] belta,  double[] gamma,
							  double[] cpuRes, double[] memRes, double[] netBW, 
							  List<SplittedModel> splittedModels, int taskNum, int devNum) {
		this.accReq = accReq;
		this.lanReq = lanReq;
		this.thrReq = thrReq;
		this.alpha = alpha;
		this.belta = belta;
		this.gamma = gamma;
		this.cpuRes = cpuRes;
		this.memRes = memRes;
		this.netBW = netBW;
		this.splittedModels = splittedModels;
		this.taskNum = taskNum;
		this.devNum = devNum;
		earlyExit = false;
	}
	
	public Matrix search(){
		List<Matrix> population = initialSchedulePopulation(initialPopulationSize);
		bestSchedule = selectBestMetricSchedule(population);
		bestMetric = calculateMetric(bestSchedule);
		if( gender_lan_metric <= lanReq[0]  && gender_thr_metric >= thrReq[0] && emotion_lan_metric <= lanReq[1] 
			&& emotion_thr_metric >= thrReq[1] && age_lan_metric <= lanReq[2]	&& age_thr_metric >= thrReq[2]) {
				earlyExit = true;
				return bestSchedule;
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
			
			Matrix curbest = selectBestMetricSchedule(survialOffSprings);
			double curbestMetric = calculateMetric(curbest);
			if(curbestMetric < bestMetric){
				bestMetric = curbestMetric;
				bestSchedule = curbest;
				
				if( gender_lan_metric <= lanReq[0]  && gender_thr_metric >= thrReq[0] && 
					emotion_lan_metric <= lanReq[1] && emotion_thr_metric >= thrReq[1] &&
					age_lan_metric <= lanReq[2]	&& age_thr_metric >= thrReq[2]) {
					earlyExit = true;
					break;
				}
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
	
	public Matrix selectBestMetricSchedule(List<Matrix> population){
		Matrix bestSchedule = null;
		double minMetric = Double.MAX_VALUE ;
		
		for (Matrix m: population){
			double metric = calculateMetric(m);
			if (metric < minMetric){
				minMetric = metric;
				bestSchedule = m.copy(); 
			}	
		}
	
		return bestSchedule;
	}
		
	public double calculateMetric(Matrix allocation){
		// device each task is assigned to
		int devCommon  = devOfTask(allocation, 0);
		int devGender  = devOfTask(allocation, 1);
		int devEmotion = devOfTask(allocation, 2);
		int devAge     = devOfTask(allocation, 3);
		
		//// GENDER:
		Model genderModel = new ModelCatlog().searchModel(splittedModels.get(1).type, splittedModels.get(1).baseType, splittedModels.get(1).frozenPoint);
		// metric of gender accuracy
		gender_acc = genderModel.accuracy;
		gender_acc_metric = alpha[0] * (accReq[0] - gender_acc) / accReq[0];
		// metric of gender latency
		gender_lan = splittedModels.get(0).latency * numOfTasksInDev(allocation, devCommon) / cpuRes[devCommon]
				          + splittedModels.get(1).latency * numOfTasksInDev(allocation, devGender) / cpuRes[devGender]
				          + splittedModels.get(0).featureSize * 8 / 4 / Math.min(netBW[devCommon], netBW[devGender]) + splittedModels.get(0).featureSize / 50 * 6.29; // unit:ms		
		gender_lan_metric = belta[0] * Math.max(0, gender_lan - lanReq[0]) / gender_lan;
		// metric of gender throughput
		gender_thr = Math.min(Math.min(1000.0 / splittedModels.get(0).latency / numOfTasksInDev(allocation, devCommon) * cpuRes[devCommon], 
											  1000.0 / splittedModels.get(1).latency / numOfTasksInDev(allocation, devGender) * cpuRes[devGender]),
											  1000.0 / (splittedModels.get(0).featureSize * 8 / 4 / Math.min(netBW[devCommon], netBW[devGender]) + splittedModels.get(0).featureSize / 50 * 6.29));
		gender_thr_metric = gamma[0] * Math.max(0, thrReq[0] - gender_thr) / thrReq[0];
		gender_metric = gender_acc_metric + gender_lan_metric + gender_thr_metric;
		
		//// EMOTION:
		Model emotionModel = new ModelCatlog().searchModel(splittedModels.get(2).type, splittedModels.get(2).baseType, splittedModels.get(2).frozenPoint);
		// metric of emotion accuracy
		emotion_acc = emotionModel.accuracy;
		emotion_acc_metric = alpha[1] * (accReq[1] - emotion_acc) / accReq[1];
		// metric of emotion latency
		emotion_lan = splittedModels.get(0).latency * numOfTasksInDev(allocation, devCommon) / cpuRes[devCommon]
				           + splittedModels.get(2).latency * numOfTasksInDev(allocation, devEmotion) / cpuRes[devEmotion]
				           + splittedModels.get(0).featureSize * 8 / 4 / Math.min(netBW[devCommon], netBW[devEmotion]) + splittedModels.get(0).featureSize / 50 * 6.29;  // unit:ms		
		emotion_lan_metric = belta[1] * Math.max(0, emotion_lan - lanReq[1]) / emotion_lan;
		// metric of emotion throughput
		emotion_thr = Math.min(Math.min(1000.0 / splittedModels.get(0).latency / numOfTasksInDev(allocation, devCommon)  * cpuRes[devCommon], 
											   1000.0 / splittedModels.get(2).latency / numOfTasksInDev(allocation, devEmotion) * cpuRes[devEmotion]),
											   1000.0 / (splittedModels.get(0).featureSize * 8 / 4 / Math.min(netBW[devCommon], netBW[devEmotion]) + splittedModels.get(0).featureSize / 50 * 6.29));
		emotion_thr_metric = gamma[1] * Math.max(0, thrReq[1] - emotion_thr) / thrReq[1];
		emotion_metric = emotion_acc_metric + emotion_lan_metric + emotion_thr_metric;
		
		//// AGE:
		Model ageModel = new ModelCatlog().searchModel(splittedModels.get(3).type, splittedModels.get(3).baseType, splittedModels.get(3).frozenPoint);
		// metric of age accuracy
		age_acc = ageModel.accuracy;
		age_acc_metric = alpha[2] * (accReq[2] - age_acc) / accReq[2];
		// metric of age latency
		age_lan = splittedModels.get(0).latency * numOfTasksInDev(allocation, devCommon) / cpuRes[devCommon] 
				       + splittedModels.get(3).latency * numOfTasksInDev(allocation, devAge) / cpuRes[devAge]
				       + splittedModels.get(0).featureSize * 8 / 4 / Math.min(netBW[devCommon], netBW[devAge]) + splittedModels.get(0).featureSize / 50 * 6.29;	//unit: ms	
		age_lan_metric = belta[2] * Math.max(0, age_lan - lanReq[2]) / age_lan;
		// metric of age throughput
		age_thr = Math.min(Math.min(1000.0 / splittedModels.get(0).latency / numOfTasksInDev(allocation, devCommon) * cpuRes[devCommon], 
										   1000.0 / splittedModels.get(3).latency / numOfTasksInDev(allocation, devAge) * cpuRes[devAge]),
										   1000.0 / (splittedModels.get(0).featureSize * 8 / 4 / Math.min(netBW[devCommon], netBW[devAge]) + splittedModels.get(0).featureSize / 50 * 6.29));
		age_thr_metric = gamma[2] * Math.max(0, thrReq[2] - age_thr) / thrReq[2];
		age_metric = age_acc_metric + age_lan_metric + age_thr_metric;
		
		// metric
		return Math.max(Math.max(gender_metric, emotion_metric), age_metric);		
	}
	
	public int devOfTask(Matrix allocation, int task) {
		int dev = -1;
		for(int i = 0; i<devNum; i++) {
			if(allocation.get(task,i)==1) {
				dev = i;
				break;
			}
		}
		return dev;
	}
	
	public int numOfTasksInDev(Matrix allocation, int dev) {
		int num = 0;
		for(int i=0; i<taskNum; i++) {
			if(allocation.get(i, dev)==1) {
				num++;
			}
		}
		return num;
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
		localBestMetric = calculateMetric(m);
        
		int randomRowNum;
		for (randomRowNum = 0; randomRowNum<(m.getRowDimension()-1);randomRowNum++){
			Matrix rowCurrent = m.getMatrix(randomRowNum, randomRowNum, 0, devNum-1);
			Matrix rowNext = m.getMatrix(randomRowNum+1, randomRowNum+1, 0, devNum-1);
			Matrix neighbor = m.copy();
			neighbor.setMatrix(randomRowNum, randomRowNum, 0, devNum-1, rowNext);
			neighbor.setMatrix(randomRowNum+1, randomRowNum+1, 0, devNum-1, rowCurrent);
			if(judgeMatrixSatCons(neighbor, memRes)){		
				double neighborMetric = 0.0;
				neighborMetric = calculateMetric(neighbor);
		    	if(neighborMetric<localBestMetric){
					localBestMetric = neighborMetric;
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
			if (judgeMatrixSatCons(offspring, memRes)){
				survivalList.add(offspring);
			}
		}
		return survivalList;
	}
	
	public boolean judgeMatrixSatCons(Matrix m, double[] constraint){
		for(int j=0; j<devNum;j++){
			double constraintJ = constraint[j];
			double colSumJ = 0.0;
			for(int i = 0;i<taskNum;i++){
				colSumJ += m.get(i, j)==1 ? splittedModels.get(i).memorySize : 0 ;
			}
			if(colSumJ>constraintJ)
				return false;
		}
		return true;
	}
	
	public Matrix generateRandomMatrix(){
		Matrix newMatrix = new Matrix(taskNum,devNum);
		double[] cons = memRes.clone();
		for(int i = 0; i < taskNum; i++){
			SplittedModel model = splittedModels.get(i);
			double memSize = model.memorySize;
			int column = RandomNumGenerator.randomInt(devNum);
			while(cons[column] < memSize){
				column = (++column)%devNum;
				
				System.out.println("I am stuck here:"+"memorysize:"+memSize+"dev:"+column+"devRes:"+cons[column]);
			}
			newMatrix.set(i,column, 1);
			cons[column] = cons[column] - memSize;
		}
		return newMatrix;
	}
	
	public List<Matrix> chooseParents(List<Matrix> population, int parentRatio){	
		Collections.sort(population, new Comparator<Matrix>() {
	        @Override
	        public int compare(Matrix m1, Matrix m2)
	        {	        	
	        	Double m1MinAvailMetric = calculateMetric(m1);
				Double m2MinAvailMetric = calculateMetric(m2);
				return m1MinAvailMetric.compareTo(m2MinAvailMetric);
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
		
		return parents;
	}

	public double getBestMetric(){
		return bestMetric;
	}
	
	public Matrix getBestSchedule(){
		return bestSchedule;
	}
	
	public boolean getEarlyExit() {
		return earlyExit;
	}

	public void printSeparateMertic() {
		System.out.println("============================");
		System.out.println("gender_acc:" + gender_acc);
		System.out.println("gender_lan:" + gender_lan);
		System.out.println("gender_thr:" + gender_thr);
		System.out.println("gender_metric:" + gender_metric);
		System.out.println("===============");
		System.out.println("emotion_acc:" + emotion_acc);
		System.out.println("emotion_lan:" + emotion_lan);
		System.out.println("emotion_thr:" + emotion_thr);	
		System.out.println("emotion_metric:" + emotion_metric);
		System.out.println("===============");
		System.out.println("age_acc:" + age_acc);
		System.out.println("age_lan:" + age_lan);
		System.out.println("age_thr:" + age_thr);
		System.out.println("age_metric:" + age_metric);
		System.out.println("===============");
		System.out.println("best_metric:" + bestMetric);		
		System.out.println("============================");
	}
}
