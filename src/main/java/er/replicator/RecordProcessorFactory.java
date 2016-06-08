package er.replicator;


import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class RecordProcessorFactory implements IRecordProcessorFactory {

    private final String outputStreamName;
    private final AmazonKinesisClient amazonKinesisClient;
    private final int maxPutRecords;

    public RecordProcessorFactory(String outputStreamName, AmazonKinesisClient client, int maxPutRecords) {
        this.outputStreamName = outputStreamName;
        this.amazonKinesisClient = client;
        this.maxPutRecords = maxPutRecords;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new RecordProcessor(outputStreamName, amazonKinesisClient, maxPutRecords);
    }
}
