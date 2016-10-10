package com.test.parser;

import org.junit.Assert;
import org.junit.Test;

public class XmlParserTest {
    @Test
    public void testParseDocumentsTag() throws Exception {
        Assert.assertEquals("<Documents>", XmlParser.parseLine("\t\t<Documents>"));
        Assert.assertEquals("</Documents>", XmlParser.parseLine("\t\t</Documents>"));
    }

    @Test
    public void testParseDocumentTag() throws Exception {
        Assert.assertEquals("\t<Document DocID=\"3.818877.G3T4II30F0UK4YM4G2XQMIIKYS451SXUA\" DocType=\"Message\" MimeType=\"message/rfc822\">",
                XmlParser.parseLine("\t\t\t<Document DocID=\"3.818877.G3T4II30F0UK4YM4G2XQMIIKYS451SXUA\" DocType=\"Message\" MimeType=\"message/rfc822\">"));
        Assert.assertEquals("\t</Document>", XmlParser.parseLine("\t\t\t</Document>"));
    }

    @Test
    public void testParseTagTo() throws Exception {
        Assert.assertEquals("\t\t<SendTo>pallen70@hotmail.com</SendTo>",
                XmlParser.parseLine("\t\t\t\t\t<Tag TagName=\"#To\" TagDataType=\"Text\" TagValue=\"pallen70@hotmail.com\"/>"));
    }

    @Test
    public void testParseTagCC() throws Exception {
        Assert.assertEquals("\t\t<SendCC>pallen70@hotmail.com</SendCC>",
                XmlParser.parseLine("\t\t\t\t\t<Tag TagName=\"#CC\" TagDataType=\"Text\" TagValue=\"pallen70@hotmail.com\"/>"));
    }

    @Test
    public void testParseExternalFile() throws Exception {
        Assert.assertEquals("\t\t<FilePath>text_000/3.818877.G3T4II30F0UK4YM4G2XQMIIKYS451SXUA.txt</FilePath>",
                XmlParser.parseLine("\t\t\t\t\t\t<ExternalFile FilePath=\"text_000\" FileName=\"3.818877.G3T4II30F0UK4YM4G2XQMIIKYS451SXUA.txt\" FileSize=\"9365\" Hash=\"BD1A973FE36F9B8743AE98DD0971EFFC\"/>"));
    }

    @Test
    public void testParseRandom() throws Exception {
        Assert.assertEquals("", XmlParser.parseLine("some random string"));
    }

}