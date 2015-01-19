package com.mobicloud.DRTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-9-19
 * Time: 14:26
 */
public class SVDTest {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SVDTest").setMaster("local[2]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile("/home/nodin/data/svd.txt");
		JavaRDD<Vector> rows = data.map(s -> {
			double[] values = Arrays.asList(SPACE.split(s))
				  .stream()
				  .mapToDouble(Double::parseDouble)
				  .toArray();
			return Vectors.dense(values);
		});

		RowMatrix mat = new RowMatrix(rows.rdd());
		Matrix pc = mat.computePrincipalComponents(3);
		RowMatrix projected = mat.multiply(pc);
		System.out.println(projected.numRows());
		System.out.println(projected.numCols());
	}
//	SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(3, true, 1.0E-9d);
//	RowMatrix U = svd.U();
//	Vector s = svd.s();
//	Matrix V = svd.V();
//	System.out.println(U);
//	System.out.println("-------------------");
//	System.out.println(s);
//	System.out.println("-------------------");
//	System.out.println(V);
//		MultivariateStatisticalSummary summary = mat.computeColumnSummaryStatistics();
//		System.out.println(summary.count());
//		System.out.println(summary.max());
//		System.out.println(summary.min());
//		System.out.println(summary.mean());
//		System.out.println(summary.numNonzeros());
//		System.out.println(summary.variance());

//	Matrix matrix = mat.computeCovariance();//?
//	System.out.println(matrix);

//		Matrix matrix = mat.computeGramianMatrix();
//		System.out.println(matrix);

//		Matrix pc = mat.computePrincipalComponents(3);
//		RowMatrix projected = mat.multiply(pc);
//		System.out.println(projected.numCols());
//		System.out.println(projected.numRows());
}
