package com.ibm.cedp.pipeline;

import com.ibm.cedp.pipeline.connector.FlinkKafkaConnector;
import com.ibm.cedp.pipeline.connector.KafkaTopic;
import com.ibm.cedp.streams.SlackTextWriteToKafkaSourceData;
import com.ibm.cedp.streams.WriteToKafkaSourceData;
import kafka.common.TopicExistsException;
import org.apache.commons.cli.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * The FlinkPreprocessModelData program parses the command line for arguments,
 * depending on the value of datasource, fetches the raw data from the appropriate Kafka topic,
 * depending on the value of processtype, pre-processes the data to generate the labels for training purposes
 * and writes the pre=processed data to another Kafka topic.
 *
 *
 * @author  Roukna Sengupta
 * @version 1.0
 * @since   2017-08-03
 */

public class FlinkPreprocessModelData {

    /**
     *  Defines datasource options for the command line.
     */

    private static Option datasource = OptionBuilder.withArgName("datasource")
            .hasArg()
            .withDescription("specify the source of data")
            .create("datasource");

    /**
     *  Defines processtype options for the command line.
     */

    private static Option processtype = OptionBuilder.withArgName("processtype")
            .hasArg()
            .withDescription("specify the type of pre-processing")
            .create("processtype");

    /**
     *  Defines topic options for the command line.
     */

    private static Option topic = OptionBuilder.withArgName("topic")
            .hasArg()
            .withDescription("specify the topic to write the preprocessed data")
            .create("topic");

    /**
     * Thrown by FlinkPreprocessModelData when some mandatory command line argument is not passed in.
     */

    @SuppressWarnings("serial")
    protected static class MissingArgumentException extends Exception {
        public MissingArgumentException(String message) {
            super(message);
        }
    }

    /**
     * Thrown by FlinkPreprocessModelData when some invalid data source is passed in.
     */

    @SuppressWarnings("serial")
    protected static class InvalidDatasourceException extends Exception {
        public InvalidDatasourceException(String message) {
            super(message);
        }
    }

    private static final FlinkKafkaConnector<String> flinkConnector = new FlinkKafkaConnector<String>();
    private static final KafkaTopic ckt = new KafkaTopic();
    private static final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final Logger logger = Logger.getLogger(FlinkPreprocessModelData.class.getName());

    public static void main (String args[]) throws IOException, MissingArgumentException, InvalidDatasourceException, Exception {

        CommandLine cmdLine = null;
        String dataSource;
        String processType;
        String writeToTopic;
        String fromTopic = null;

        Options options = new Options();

        options.addOption(datasource);
        options.addOption(topic);
        options.addOption(processtype);

        CommandLineParser parser = new BasicParser();

        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException exp) {
            logger.log( Level.ERROR, exp.getMessage(), exp);
        }

        /**
         * Parses the command line for argument datasource.
         */

        if (cmdLine.hasOption("datasource")) {

            dataSource = cmdLine.getOptionValue("datasource");
            logger.log(Level.INFO, "Parsed command line for param. --datasource" + dataSource);
        }
        else
            throw new MissingArgumentException("Argument datasource missing");

        /**
         * Parses the command line for argument processtype.
         */

        if (cmdLine.hasOption("processtype")) {

            processType = cmdLine.getOptionValue("processtype");
            logger.log(Level.INFO, "Parsed command line for param. --processtype" + processType);
        }
        else
            throw new MissingArgumentException("Argument processtype missing");

        /**
         * Parses the command line for argument topic.
         */

        if (cmdLine.hasOption("topic")) {

            writeToTopic = cmdLine.getOptionValue("topic");
            logger.log(Level.INFO, "Parsed command line for param. --topic" + writeToTopic);
        }
        else
            throw new MissingArgumentException("Argument topic missing");

        processData(dataSource, processType,fromTopic, writeToTopic);

        System.exit(0);
    }

    /**
     * Depending on datasource, consumes data stream from kafka queue(topic), pre-process it as per the processtype
     * and writes the preprocessed data back to Kafka for consumption by the ML models.
     *
     * @param dataSource - source of the data that needs to be read
     * @param processType - type of processing need to done to generate labels for the data
     * @param fromTopic - the topic from which to read the data
     * @param writeToTopic - the topic to which the processed data would be written
     * @throws InvalidDatasourceException
     */

    protected static void processData(String dataSource, String processType, String fromTopic, String writeToTopic)  throws InvalidDatasourceException, Exception{

        WriteToKafkaSourceData wDataSrc = null;
        Properties p = new Properties();

        switch (dataSource) {

            case "slacktext":

                switch (processType) {

                    case "channel":

                        // topic to read the data from
                        fromTopic = "cedp_slacktext";

                        try {
                            // create target topic where preprocessed data would be written to
                            p.setProperty("numOfPartitions", "3");
                            ckt.createTopic(writeToTopic, p);
                            logger.log(Level.INFO, "Topic " + writeToTopic + " created.");
                        }
                        catch (TopicExistsException e) {
                            logger.log(Level.ERROR, e.getMessage(), e);
                        }

                        wDataSrc = new SlackTextWriteToKafkaSourceData();
                        break;

                    case "processtype2":
                        // Some other processing type
                        // To be implemented
                        break;

                    case "processtype3":
                        // Some other processing type
                        // To be implemented
                        break;

                    /* Add more processing type depending as per requirements. */

                    default:

                }

            case "slackurl":
                // To be implemented
                break;

            case "slackstream":
                // To be implemented
                break;

            // Add more data sources as per requirements

            default:
                throw new InvalidDatasourceException("Invalid datasource "+ dataSource);

        }

        try {
            wDataSrc.writeToKafkaSourceData(flinkConnector, streamEnv, fromTopic, writeToTopic);
        } catch (Exception e) {
            logger.log(Level.ERROR, e.getMessage(), e);
            throw e;
        }
    }
}
