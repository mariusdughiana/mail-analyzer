package com.test.parser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class MailParser {

    public static int getMessageWordsNo(String filePath) throws IOException {


        boolean isHeader = true;
        int words = 0;
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        for (String line: lines) {
            if (line.startsWith("X-ZLID: ")) {
                isHeader = false;
                continue;
            }
            if(!isHeader) {

                if (line.equals("***********")) {
                    return words;
                }
                words += line.split(" ").length;
            }
        }
        return words;
    }
}
