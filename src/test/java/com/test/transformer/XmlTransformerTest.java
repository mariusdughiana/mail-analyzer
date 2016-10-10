package com.test.transformer;

import com.test.comparator.TupleComparatorByVal;
import com.test.parser.MailParserTest;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

public class XmlTransformerTest {

    private static String newxml;
    private static JavaSparkContext sc;
    private static Dataset ds;

    @BeforeClass
    public static void setup() {
        String filePath = XmlTransformerTest.class.getResource("/zl_allen-p_863_K14C_000.xml").getPath();
        SparkConf conf = new SparkConf().setAppName("Test").setMaster("local");
        sc = new JavaSparkContext(conf);
        newxml = XmlTransformer.transformXml(sc, filePath);
        SQLContext sqlContext = new SQLContext(sc);

        ds = sqlContext.read()
                               .format("com.databricks.spark.xml")
                               .option("rowTag", "Document")
                               .load(newxml);
    }
    @AfterClass
    public static void clear() throws IOException {
        FileUtils.deleteDirectory(new File("newxml.xml"));
        sc.stop();
    }


    @Test
    public void transformXml() throws Exception {


        byte[] bytes = Files.readAllBytes(Paths.get(newxml, "part-00000"));

        Assert.assertEquals("<Documents>\n" +
                "\t<Document DocID=\"3.818877.G3T4II30F0UK4YM4G2XQMIIKYS451SXUA\" DocType=\"Message\" MimeType=\"message/rfc822\">\n" +
                "\t\t<SendTo>pallen70@hotmail.com</SendTo>\n" +
                "\t\t<SendCC>pallen70@hotmail.com</SendCC>\n" +
                "\t\t<FilePath>text_000/3.818894.G0PM2HB5IBSYKFT1XHJ1FHAMRIBTEBWMB.txt</FilePath>\n" +
                "\t</Document>\n" +
                "\t<Document DocID=\"3.824069.AXUT5DDC35BW0CN3OYDX5HCO0Z3DNELQB\" DocType=\"Message\" MimeType=\"message/rfc822\">\n" +
                "\t\t<FilePath>text_000/3.824069.AXUT5DDC35BW0CN3OYDX5HCO0Z3DNELQB.txt</FilePath>\n" +
                "\t</Document>\n" +
                "\t<Document DocID=\"3.824070.MUCRTIGATEKE1YRC4VFOAWOXYOLUYHICA\" DocType=\"Message\" MimeType=\"message/rfc822\">\n" +
                "\t\t<FilePath>text_000/3.824070.MUCRTIGATEKE1YRC4VFOAWOXYOLUYHICA.txt</FilePath>\n" +
                "\t</Document>\n" +
                "\t<Document DocID=\"3.824071.KUTHIRTYFDH3DUTSVJ14LWJTPMHZVMTTB\" DocType=\"Message\" MimeType=\"message/rfc822\">\n" +
                "\t\t<FilePath>text_000/3.824071.KUTHIRTYFDH3DUTSVJ14LWJTPMHZVMTTB.txt</FilePath>\n" +
                "\t</Document>\n" +
                "\t<Document DocID=\"3.824072.DLX5BRKHIAHWZSD5QYRIZOA31RSTGBYXA\" DocType=\"Message\" MimeType=\"message/rfc822\">\n" +
                "\t\t<FilePath>text_000/3.824072.DLX5BRKHIAHWZSD5QYRIZOA31RSTGBYXA.txt</FilePath>\n" +
                "\t</Document>\n" +
                "\t<Document DocID=\"3.824073.KTYF3OV0B5W4SDJXPR1BD4RTENKLL5PMB\" DocType=\"Message\" MimeType=\"message/rfc822\">\n" +
                "\t\t<FilePath>text_000/3.824073.KTYF3OV0B5W4SDJXPR1BD4RTENKLL5PMB.txt</FilePath>\n" +
                "\t</Document>\n" +
                "\t<Document DocID=\"3.824074.F4Y5FYV2RNDLO3VHIJYN12DFDMCNCJMUA\" DocType=\"Message\" MimeType=\"message/rfc822\">\n" +
                "\t\t<FilePath>text_000/3.824074.F4Y5FYV2RNDLO3VHIJYN12DFDMCNCJMUA.txt</FilePath>\n" +
                "\t</Document>\n" +
                "\t<Document DocID=\"3.824075.BRY1DLED0CERLI1G02VGBUDMSP05AA02A\" DocType=\"Message\" MimeType=\"message/rfc822\">\n" +
                "\t\t<FilePath>text_000/3.824075.BRY1DLED0CERLI1G02VGBUDMSP05AA02A.txt</FilePath>\n" +
                "\t</Document>\n" +
                "\t<Document DocID=\"3.824076.O1VTOAI3EAE4WN02UKUWOR1ZM4B5SXUMA\" DocType=\"Message\" MimeType=\"message/rfc822\">\n" +
                "\t\t<FilePath>text_000/3.824076.O1VTOAI3EAE4WN02UKUWOR1ZM4B5SXUMA.txt</FilePath>\n" +
                "\t</Document>\n" +
                "</Documents>\n", new String(bytes));

    }

    @Test
    public void countMailReceivers() throws Exception {

        JavaPairRDD<String, Double> counts = XmlTransformer.countMailReceivers(ds);

        final StringBuffer result = new StringBuffer();

        counts.takeOrdered(100, new TupleComparatorByVal()).forEach(s -> result.append(s._1() + ": " + s._2() + "\n"));

        Assert.assertEquals("pallen70@hotmail.com: 1.5\n", result.toString());
    }

    @Test
    public void getContentForTagInMessage() throws Exception {

        JavaRDD<String> receivers = XmlTransformer.getContentForTagInMessage(ds, "SendTo", ",");
        Assert.assertEquals("pallen70@hotmail.com", receivers.first());
        Assert.assertEquals(1, receivers.count());

        List<String> mailFiles = XmlTransformer.getContentForTagInMessage(ds, "FilePath", null).collect();
        Assert.assertEquals(9, mailFiles.size());
        Assert.assertEquals("text_000/3.818894.G0PM2HB5IBSYKFT1XHJ1FHAMRIBTEBWMB.txt", mailFiles.get(0));
        Assert.assertEquals("text_000/3.824069.AXUT5DDC35BW0CN3OYDX5HCO0Z3DNELQB.txt", mailFiles.get(1));
        Assert.assertEquals("text_000/3.824070.MUCRTIGATEKE1YRC4VFOAWOXYOLUYHICA.txt", mailFiles.get(2));
        Assert.assertEquals("text_000/3.824071.KUTHIRTYFDH3DUTSVJ14LWJTPMHZVMTTB.txt", mailFiles.get(3));
        Assert.assertEquals("text_000/3.824072.DLX5BRKHIAHWZSD5QYRIZOA31RSTGBYXA.txt", mailFiles.get(4));
        Assert.assertEquals("text_000/3.824073.KTYF3OV0B5W4SDJXPR1BD4RTENKLL5PMB.txt", mailFiles.get(5));
        Assert.assertEquals("text_000/3.824074.F4Y5FYV2RNDLO3VHIJYN12DFDMCNCJMUA.txt", mailFiles.get(6));
        Assert.assertEquals("text_000/3.824075.BRY1DLED0CERLI1G02VGBUDMSP05AA02A.txt", mailFiles.get(7));
        Assert.assertEquals("text_000/3.824076.O1VTOAI3EAE4WN02UKUWOR1ZM4B5SXUMA.txt", mailFiles.get(8));
    }

}