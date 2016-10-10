package com.test.transformer;

import com.test.parser.XmlParser;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class XmlTransformer {

    public static String transformXml(JavaSparkContext sc, String filePath) {
        JavaRDD<String> mailDetails = sc.textFile(filePath);
        mailDetails.map((Function<String, Object>) line -> XmlParser.parseLine(line))
                   .filter(line -> !line.equals(""))
                   .saveAsTextFile("newxml.xml");
        return "newxml.xml";
    }

    public static JavaPairRDD<String, Double> countMailReceivers(Dataset ds, String... tags) {
        JavaRDD<String> receivers = getContentForTagInMessage(ds, "SendTo", ",");
        JavaPairRDD<String, Double> pairs = receivers.mapToPair(s -> new Tuple2(s, 1.0));
        if (containsColumn(ds, "SendCC")) {
            pairs = pairs.union(getContentForTagInMessage(ds, "SendCC", ",").mapToPair(s -> new Tuple2(s, 0.5)));
        }
        return pairs.reduceByKey((a, b) -> a + b);
    }

    public static JavaRDD<String> getContentForTagInMessage(Dataset ds, String tagName, String separator) {
        return ds.select(tagName).where("_DocType = 'Message'")
                 .javaRDD().map(o -> {
                    Set<String> results = new HashSet<>();
                    if (((GenericRowWithSchema)o).get(0) != null && !((GenericRowWithSchema)o).get(0).equals("null")) {
                        if (separator != null) {
                            String[] resultsPerEntry = ((GenericRowWithSchema)o).get(0).toString().split(separator);
                            results.addAll(Arrays.asList(resultsPerEntry));
                        } else {
                            results.add(((GenericRowWithSchema)o).get(0).toString());
                        }
                    }
                    return results;
                }).flatMap((FlatMapFunction) o -> ((Set)o).iterator());
    }

    private static boolean containsColumn(Dataset ds, String columnName) {
        return Arrays.asList(ds.columns()).contains(columnName);
    }
 }
