package com.test.parser;

import org.apache.commons.lang.StringUtils;

public class XmlParser {


    public static String parseLine(String line) {
        if (line.equals("\t\t<Documents>")) {
            return "<Documents>";
        }
        if (line.startsWith("\t\t\t<Document DocID=")) {
            return line.replaceAll("\t\t\t", "\t");
        }
        if (line.startsWith("\t\t\t\t\t<Tag TagName=\"#To\"")) {
            return "\t\t<SendTo>"+StringUtils.substringBetween(line, "TagValue=\"", "\"/>")+"</SendTo>";
        }
        if (line.startsWith("\t\t\t\t\t<Tag TagName=\"#CC\"")) {
            return "\t\t<SendCC>"+StringUtils.substringBetween(line, "TagValue=\"", "\"/>")+"</SendCC>";

        }
        if (line.startsWith("\t\t\t\t\t\t<ExternalFile FilePath=\"text")) {
            String[] lineElem = line.split(" ");
            String path = lineElem[1].split("=")[1].replaceAll("\"", "");
            String fileName = lineElem[2].split("=")[1].replaceAll("\"", "");
            return "\t\t<FilePath>"+path+"/"+fileName+ "</FilePath>";
        }
        if (line.equals("\t\t\t</Document>")) {
            return line.replaceAll("\t\t\t", "\t");
        }
        if (line.equals("\t\t</Documents>")) {
            return "</Documents>";
        }

        return "";
    }
}
