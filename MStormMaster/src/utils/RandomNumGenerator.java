package utils;

import java.util.Random;

public class RandomNumGenerator {
	public static int randomInt(int bound){
		long seed = System.nanoTime();
		Random rand = new Random(seed);
		return rand.nextInt(bound);
	}
	public static double randomDouble(double bound){
		long seed = System.nanoTime();
		Random rand = new Random(seed);
		return rand.nextDouble()*bound;
	}
}

