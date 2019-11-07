package nimbusscheduler;

import Jama.Matrix;

public class REMStormAdvancedGeneticSearch {
	// inputs
	int numOfComp;
	int numOfTask;
	int numOfDev;
	int[] parallelNum;
	int[] execCap;
	double[] availability;
	Matrix t2tTrafficDouble;
	
	// weights
	double weightAvailability = 0.5;
	double weightDiversity = 0.5;
	double weightTraffic = 0.0;
	
	// Intermediate parameters
	double minAvail;
	double maxAvail;
	double minDiversity;
	double maxDiversity;
	double minTraffic;
	double maxTraffic;
	
	public REMStormAdvancedGeneticSearch(double[][] t2tTraffic, int[] parallelNumber, int[] execCapacity, double[] devAvail) {
		t2tTrafficDouble = new Matrix(t2tTraffic);
		parallelNum = parallelNumber;
		execCap = execCapacity;
		availability = devAvail;
		numOfComp = parallelNum.length;
		numOfTask = t2tTraffic[0].length;
		numOfDev = devAvail.length;
	}

	public Matrix search() {		
		REMStormGeneticSearch rEMStormGeneticSearch;

		rEMStormGeneticSearch = new REMStormGeneticSearch(t2tTrafficDouble, parallelNum, 
				execCap, availability, numOfComp, numOfTask, numOfDev, REMStormGeneticSearch.MINAVAIL);
        rEMStormGeneticSearch.search();
        minAvail = rEMStormGeneticSearch.getBestMetric();
        //System.out.println("minAvail: " + minAvail);
        
		rEMStormGeneticSearch = new REMStormGeneticSearch(t2tTrafficDouble, parallelNum, 
                execCap, availability, numOfComp, numOfTask, numOfDev, REMStormGeneticSearch.MAXAVAIL);
		rEMStormGeneticSearch.search();
		maxAvail = rEMStormGeneticSearch.getBestMetric();
		//System.out.println("maxAvail: " + maxAvail);
		
		rEMStormGeneticSearch = new REMStormGeneticSearch(t2tTrafficDouble, parallelNum, 
		        execCap, availability, numOfComp, numOfTask, numOfDev, REMStormGeneticSearch.MINDIVERSITY);
		rEMStormGeneticSearch.search();
		minDiversity= rEMStormGeneticSearch.getBestMetric();
		//System.out.println("minDiversity: " + minDiversity);
		
		rEMStormGeneticSearch = new REMStormGeneticSearch(t2tTrafficDouble, parallelNum, 
		        execCap, availability, numOfComp, numOfTask, numOfDev, REMStormGeneticSearch.MAXDIVERSITY);
		rEMStormGeneticSearch.search();
		maxDiversity = rEMStormGeneticSearch.getBestMetric();
		//System.out.println("maxDiversity: " + maxDiversity);
		
		rEMStormGeneticSearch = new REMStormGeneticSearch(t2tTrafficDouble, parallelNum, 
		        execCap, availability, numOfComp, numOfTask, numOfDev, REMStormGeneticSearch.MINTRAFFIC);
		rEMStormGeneticSearch.search();
		minTraffic = rEMStormGeneticSearch.getBestMetric();
		//System.out.println("minTraffic: " + minTraffic);
		
		rEMStormGeneticSearch = new REMStormGeneticSearch(t2tTrafficDouble, parallelNum, 
		        execCap, availability, numOfComp, numOfTask, numOfDev, REMStormGeneticSearch.MAXTRAFFIC);
		rEMStormGeneticSearch.search();
		maxTraffic = rEMStormGeneticSearch.getBestMetric();
		//System.out.println("maxTraffic: " + maxTraffic);
		
		rEMStormGeneticSearch = new REMStormGeneticSearch(t2tTrafficDouble, parallelNum, 
		        execCap, availability, numOfComp, numOfTask, numOfDev, REMStormGeneticSearch.MAXTRARESIL, 
		        weightAvailability, weightDiversity, weightTraffic, maxAvail, minAvail, maxDiversity, minDiversity, maxTraffic, minTraffic);
		rEMStormGeneticSearch.search();
		
		return rEMStormGeneticSearch.getBestSchedule();
	}
}
