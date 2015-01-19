package com.mobicloud.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-9-17
 * Time: 13:32
 */
public class StatisticsTest {

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

		MultivariateStatisticalSummary summary = Statistics.colStats(parsedData.rdd());
		System.out.println("均值:"+summary.mean());
		System.out.println("方差:"+summary.variance());
		System.out.println("非零统计量个数:"+summary.numNonzeros());
		System.out.println("总数:"+summary.count());
		System.out.println("最大值:"+summary.max());
		System.out.println("最小值:"+summary.min());
	}
}
