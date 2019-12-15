package com.lenss.mstorm.utils;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.io.IOException;

public class TAGs {
     public static void initLogger(String loggerFilePath) throws IOException {
          //First let the logger show the messages to System.out
          Logger logger = Logger.getRootLogger();
          logger.addAppender(new ConsoleAppender(new PatternLayout("[%-5p] %d (%c{1}): %m%n"), "System.out"));
          logger.info("Trying to initialize logger at "+loggerFilePath);
          PatternLayout layout = new PatternLayout("[%-5p] %d (%c{1}): %m%n");
          RollingFileAppender appender = new RollingFileAppender(layout, loggerFilePath);
          appender.setName("myFirstLog");
          appender.setMaxFileSize("4GB");
          appender.activateOptions();
          logger.addAppender(appender);
          logger.info("\n\n======================== Starting new Process ============================");
          logger.info("Logger initialized at "+loggerFilePath);
     }
}
