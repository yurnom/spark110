package com.mobicloud.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-9-17
 * Time: 14:29
 */
public class CorrelationsTest {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Statistics").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile("/home/nodin/data/statistics.txt");
		JavaRDD<Vector> parsedData = data.map(s -> {
			double[] values = Arrays.asList(SPACE.split(s))
				  .stream()
				  .mapToDouble(Double::parseDouble)
				  .toArray();
			return Vectors.dense(values);
		});

		JavaDoubleRDD seriesX = parsedData.mapToDouble(vector -> vector.apply(0));
		JavaDoubleRDD seriesY = parsedData.mapToDouble(vector -> vector.apply(1));

		System.out.println(Statistics.corr(seriesX.srdd(), seriesY.srdd(), "pearson"));
		System.out.println(Statistics.corr(seriesX.srdd(), seriesY.srdd(), "spearman"));
		System.out.println("------------");
		System.out.println(Statistics.corr(parsedData.rdd(), "pearson"));
		System.out.println("------------");
		System.out.println(Statistics.corr(parsedData.rdd(), "spearman"));
	}
}
