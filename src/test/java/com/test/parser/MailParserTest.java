package com.test.parser;

import org.junit.Assert;
import org.junit.Test;

public class MailParserTest {
    @Test
    public void getMessageWordsNo() throws Exception {
        String filePath = MailParserTest.class.getResource("/3.818894.G0PM2HB5IBSYKFT1XHJ1FHAMRIBTEBWMB.txt").getPath();
        Assert.assertEquals(61, MailParser.getMessageWordsNo(filePath));
    }

}