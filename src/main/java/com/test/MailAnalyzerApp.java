package com.test;

import com.test.comparator.TupleComparatorByVal;
import com.test.parser.MailParser;
import com.test.transformer.XmlTransformer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class MailAnalyzerApp {
    public static void main(String[] args) throws IOException {

        LocalDateTime startTime = LocalDateTime.now();

        System.out.println("========================================================================================>");
        System.out.println("Starting mail analyzer time: " + startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        System.out.println("========================================================================================>");

        Logger.getLogger("org").setLevel(Level.OFF);

        if (args.length == 0) {
            System.out.println("You have to specify the path to emails data!!!");
            System.exit(-1);
        }

        String path = args[0];

        File dirPath = new File(path);


        SparkConf conf = new SparkConf().setAppName("Mail Analyzer").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Accumulator<Integer> totalWords = sc.accumulator(0);
        Accumulator<Integer> totalMails = sc.accumulator(0);

        JavaPairRDD<String, Double> allReceivers = null;

        List<String> newXmls = new ArrayList<>();

        for (String fileName: dirPath.list(new SuffixFileFilter(".xml"))) {

            System.out.print("Processing file: " + fileName  + " ... ");

            String newXmlFile = XmlTransformer.transformXml(sc, dirPath + "/" + fileName);
            newXmls.add(newXmlFile);
            SQLContext sqlContext = new SQLContext(sc);

            Dataset ds = sqlContext.read()
                                   .format("com.databricks.spark.xml")
                                   .option("rowTag", "Document")
                                   .load(newXmlFile);

            JavaPairRDD<String, Double> fileReceivers = XmlTransformer.countMailReceivers(ds);
            if (allReceivers == null) {
                allReceivers = fileReceivers;
            } else {
                allReceivers = allReceivers.union(fileReceivers).reduceByKey((v1, v2) -> v1+v2);
            }

            JavaRDD<String> mailFiles = XmlTransformer.getContentForTagInMessage(ds, "FilePath", null);
            mailFiles.filter(fn -> Files.exists(Paths.get(dirPath.getPath(), fn)))
                     .foreach((VoidFunction<String>) s -> {
                totalMails.$plus$eq(1);
                totalWords.$plus$eq(MailParser.getMessageWordsNo(dirPath.getPath() +"/"+s));
            });
            System.out.println("100%");

        }
        printResults(startTime, totalWords, totalMails, allReceivers.takeOrdered(100, new TupleComparatorByVal()));

        deleteXmls(newXmls);
    }

    private static void deleteXmls(List<String> newXmls) {
        newXmls.forEach(newXml -> {
            try {
                FileUtils.deleteDirectory(new File(newXml));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static void printResults(LocalDateTime startTime, Accumulator<Integer> totalWords, Accumulator<Integer>
            totalMails, List<Tuple2<String, Double>> counts) {
        System.out.println("========================================================================================>");
        System.out.println("Checking for first 100 mail receivers...");
        System.out.println("========================================================================================>");

        counts.forEach(s -> System.out.println(s._1() + ": " + s._2()));

        System.out.println("========================================================================================>");
        System.out.println("Computing average words per message.........");
        System.out.println("========================================================================================>");

        System.out.println("Avg words per mail is: " + totalWords.value()/totalMails.value());

        System.out.println("========================================================================================>");

        LocalDateTime finishingTime = LocalDateTime.now();
        System.out.println("Finishing time: " + finishingTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        System.out.println("Elapsed time: " + ChronoUnit.HOURS.between(startTime, finishingTime) + ":" + ChronoUnit.MINUTES.between(startTime, finishingTime) + ":" + ChronoUnit.SECONDS.between(startTime, finishingTime));
        System.out.println("========================================================================================>");
    }
}