package org.jikeshij.zly.Distcp;

import org.apache.commons.cli.*;
import org.apache.hbase.thirdparty.org.apache.commons.cli.DefaultParser;

import java.util.List;

public class OptionParser {
    private boolean ignoreFlag;
    private int maxConcurrency;
    private String sourcePath;
    private String destPath;

    public boolean isIgnoreFlag() {
        return ignoreFlag;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getDestPath() {
        return destPath;
    }

    public void parseOption(String[] args) throws ParseException{
        Options options = new Options();
        options.addOption(Options.builder("i")
                .longOpt("ignore")
                .argName("ignore")
                .hasArg()
                .desc("Ignore the failure")
                .build());

        options.addOption(Option.builder("m")
                .longOpt("maxConcurrent")
                .argName("maxConcurrent")
                .hasArg()
                .desc("Maximum concurrency")
                .build());

        options.addOption(Option.builder("h")
                .longOpt("help")
                .desc("Show this help message and exit program")
                .build());build

        CommandLineParser parser = (CommandLineParser) new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = parser.parse(options, args);

        List<String> noFlagArgs = cmd.getArgList();

        this.sourcePath = noFlagArgs.get(0);
        this.destPath = noFlagArgs.get(1);

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp("SparkDistCp", options, true);
            System.exit(1);
        }

        if (cmd.hasOption("h")) {
            formatter.printHelp("SparkDistCp", options, true);
            System.exit(0);
        }

        // 解析ignoreFlag, 是否忽略错误
        if (cmd.hasOption("i")) {
            try {
                this.ignoreFlag = Boolean.parseBoolean(cmd.getOptionValue("i"));
            } catch (Exception e) {
                System.err.println(e.toString());
                formatter.printHelp("SparkDistCp", options, true);
                System.exit(1);
            }
        }

        // 解析RDD的分区数
        if (cmd.hasOption("m")) {
            try {
                this.maxConcurrency = Integer.parseInt(cmd.getOptionValue("m"));
            } catch (Exception e) {
                System.err.println(e.toString());
                formatter.printHelp("SparkDistCp", options, true);
                System.exit(1);
            }
        }
    }

    public OptionParser()  {

    }
}