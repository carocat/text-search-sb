package com.sb.search.model;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;
import java.util.UUID;

public class ModelModule extends AbstractModule {

    private final String entryPoint;

    public ModelModule(String entryPoint) {
        this.entryPoint = entryPoint;
    }


    @Override
    protected void configure() {

    }


    /**
     * @return Provides spark session
     */
    @Provides
    @Singleton
    public SparkSession sparkSession() {

        Properties properties = new Properties();
        properties.setProperty("log4j.rootLogger", "INFO,console");
        properties.setProperty("log4j.appender.console", "org.apache.log4j.ConsoleAppender");
        properties.setProperty("log4j.appender.console.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.console.layout.ConversionPattern", "%d %p (%t) [%c] - %m%n");
        properties.setProperty("log4j.logger.org.apache.spark", "ERROR");
        // Settings to quiet third party logs that are too verbose
        properties.setProperty("log4j.logger.org.spark_project.jetty", "WARN");
        properties.setProperty("log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle", "ERROR");
        properties.setProperty("log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper", "INFO");
        properties.setProperty("log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter", "INFO");
        properties.setProperty("log4j.logger.org.apache.parquet", "ERROR");
        properties.setProperty("log4j.logger.parquet", "ERROR");
        //SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
        properties.setProperty("log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler", "FATAL");
        properties.setProperty("log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry", "ERROR");

        PropertyConfigurator.configure(properties);

        return SparkSession
                .builder()
                .appName("Text Search: Entry Point Class: " + entryPoint)
                .config("spark.sql.crossJoin.enabled", "true")
                .config("spark.sql.parquet.compression.codec", "gzip")
                .config("parquet.enable.summary-metadata", "true")
                .enableHiveSupport()
                .getOrCreate();
    }

    @Provides
    @Singleton
    private AmazonS3 s3Client() {
        return AmazonS3Client.builder().withCredentials(new DefaultAWSCredentialsProviderChain()).build();
    }

    @Provides
    @Singleton
    private AmazonDynamoDB amazonDynamoDBClient() {
        if ("DATABRICKS".equals(getPlatform())) {
            STSAssumeRoleSessionCredentialsProvider sTSAssumeRoleSessionCredentialsProvider =
                    new STSAssumeRoleSessionCredentialsProvider
                            .Builder("arn:aws:iam::4464-3928-7457:role/aws_big_data", "big_data_" + UUID.randomUUID().toString())
                            .build();


            return AmazonDynamoDBClient.builder()
                    .withRegion(Region.getRegion(Regions.US_EAST_1).getName())
                    .withCredentials(sTSAssumeRoleSessionCredentialsProvider)
                    .build();
        } else {
            return AmazonDynamoDBClient.builder().withRegion(Region.getRegion(Regions.US_EAST_1).getName()).build();
        }
    }

    public static String getPlatform() {
        String platform = System.getProperty("platform");
        if (StringUtils.isEmpty(platform)) {
            throw new IllegalArgumentException("Please specify the platform to run");
        }
        return platform;
    }
}
