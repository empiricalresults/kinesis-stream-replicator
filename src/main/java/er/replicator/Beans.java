package er.replicator;


import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
public class Beans {

    private static final Log log = LogFactory.getLog(Beans.class);

    @Bean
    public RecordProcessorFactory recordProcessorFactory(Config config) {
        AWSCredentialsProvider credentialsProvider = credentialsProviderForClass(config.getOutputStreamCredentialsProvider());

        AmazonKinesisClient client = new AmazonKinesisClient(credentialsProvider);
        client.setRegion(Region.getRegion(Regions.fromName(config.getOutputStreamAwsRegion())));

        // quick call to make sure the output stream exists
        client.describeStream(config.getKinesisOutputStream());

        return new RecordProcessorFactory(config.getKinesisOutputStream(), client);
    }

    @Bean
    public KinesisClientLibConfiguration kinesisClientLibConfiguration(Config config) {

        KinesisClientLibConfiguration c = new KinesisClientLibConfiguration(
                config.getAppName(),
                config.getKinesisInputStream(),
                credentialsProviderForClass(config.getInputStreamCredentialsProvider()),
                credentialsProviderForClass(config.getDynamoCredentialsProvider()),
                credentialsProviderForClass(config.getCloudwatchCredentialsProvider()),
                UUID.randomUUID().toString()
        );

        c.withInitialPositionInStream(InitialPositionInStream.valueOf(config.getInitialPositionInStream()))
                .withInitialLeaseTableReadCapacity(config.getDynamoReadCapacity())
                .withInitialLeaseTableWriteCapacity(config.getDynamoWriteCapacity())
                .withIdleTimeBetweenReadsInMillis(100)
                .withMaxRecords(config.getMaxKinesisGetRecords())
                .withRegionName(config.getInputStreamAwsRegion());

        return c;
    }

    @Bean
    public Worker worker(KinesisClientLibConfiguration config,
                         RecordProcessorFactory recordProcessorFactory) {
        return new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(config)
                .build();
    }

    public static AWSCredentialsProvider credentialsProviderForClass(String clazz) {
        if (clazz == null) {
            return new DefaultAWSCredentialsProviderChain();
        }

        try {
            return (AWSCredentialsProvider) Class.forName(clazz).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
