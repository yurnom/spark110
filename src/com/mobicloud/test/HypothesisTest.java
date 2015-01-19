package com.mobicloud.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-9-17
 * Time: 16:40
 */
public class HypothesisTest {
	public static void main(String[] args) {
	//		Vector vec1 = Vectors.dense(1,7,2,3,18);
	//		Vector vec2 = Vectors.dense(7,8,6,7,9);
	//		ChiSqTestResult goodnessOfFitTestResult1 = Statistics.chiSqTest(vec1);
	//		ChiSqTestResult goodnessOfFitTestResult2 = Statistics.chiSqTest(vec2);
	//		System.out.println(goodnessOfFitTestResult1);
	//		System.out.println(goodnessOfFitTestResult2);

		Matrix matrix = new DenseMatrix(2, 2, new double[]{43,9,44,4});
		ChiSqTestResult independenceTestResult = Statistics.chiSqTest(matrix);
		System.out.println(independenceTestResult);
	}
}
