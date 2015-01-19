package com.mobicloud.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.random.RandomRDDs;

/**
 * Created with IntelliJ IDEA.
 * User: He Qi
 * Date: 14-9-17
 * Time: 17:53
 */
public class RandomTest {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Statistics").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaDoubleRDD n = RandomRDDs.normalJavaRDD(sc, 100L, 1);
		JavaDoubleRDD p = RandomRDDs.poissonJavaRDD(sc, 10, 100L);
		JavaDoubleRDD u = RandomRDDs.uniformJavaRDD(sc, 100);

		JavaDoubleRDD v = n.mapToDouble(x -> 1.0 + 2.0 * x);
		for(Double d : v.collect()) {
			System.out.println(d);
		}
		for(Double d : p.collect()) {
			System.out.println(d);
		}
		JavaDoubleRDD g = u.mapToDouble(x -> 2.0 + 3.0 * x);
		for(Double d : u.collect()) {
			System.out.println(d);
		}
	}
}
