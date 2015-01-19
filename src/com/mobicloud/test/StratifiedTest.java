package com.mobicloud.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-9-17
 * Time: 16:06
 */
public class StratifiedTest {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Statistics").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile("/home/nodin/data/sampling.txt");
		JavaPairRDD<String, Integer> parsedData = data.mapToPair(s -> {
			String[] values = s.split("\t");
			return new Tuple2<>(values[0], Integer.parseInt(values[1]));
		});

		Map<String, Object> fractions = new HashMap<>();
		fractions.put("man", 0.5);
		fractions.put("woman", 0.5);
		fractions.put("child", 0.5);
		fractions.put("baby", 0.3);
		JavaPairRDD<String, Integer> approxSample = parsedData.sampleByKey(false, fractions);
		JavaPairRDD<String, Integer> exactSample = parsedData.sampleByKeyExact(false, fractions);

		print(approxSample.collect());
		print(exactSample.collect());
	}

	public static void print(List list) {
		for(Object o : list) {
			Tuple2<String, Integer> t = (Tuple2<String, Integer>)o;
			System.out.println(t._1()+":"+t._2());
		}
	}
}
