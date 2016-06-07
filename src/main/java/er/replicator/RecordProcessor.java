package er.replicator;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class RecordProcessor implements IRecordProcessor {

    public static final Log log = LogFactory.getLog(RecordProcessor.class);

    private final String outputStreamName;
    private final AmazonKinesisClient amazonKinesisClient;
    private final List<PutRecordsRequestEntry> currentRecords = Lists.newLinkedList();

    private String shardId;
    private int recordCount;
    private int byteCount;

    // we'll cap our requests (kinesis supports up to 5mb per request)
    private static final int MAX_BYTES = 3 * 1024 * 1024;

    private static final int MAX_RECORDS = 500;

    public RecordProcessor(String outputStreamName, AmazonKinesisClient client) {
        this.outputStreamName = outputStreamName;
        this.amazonKinesisClient = client;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.getShardId();
        log.info("Starting record processor for stream " + shardId);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        IRecordProcessorCheckpointer checkpointer = processRecordsInput.getCheckpointer();

        Record lastRecord = null;
        for (Record record : processRecordsInput.getRecords()) {

            byte[] data = record.getData().array();

            PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
            entry.setData(record.getData());
            entry.setPartitionKey(record.getPartitionKey());

            this.currentRecords.add(entry);
            this.recordCount += 1;
            this.byteCount += data.length;

            lastRecord = record;

            if (shouldCheckpoint()) {
                checkpoint(checkpointer, lastRecord);
            }
        }

        if (shouldCheckpoint() && lastRecord != null) {
            checkpoint(checkpointer, lastRecord);
        }
    }

    public boolean shouldCheckpoint() {
        return ((recordCount >= MAX_RECORDS) || (byteCount >= MAX_BYTES));
    }

    void checkpoint(IRecordProcessorCheckpointer checkpointer, Record lastRecord) {

        PutRecordsRequest request = new PutRecordsRequest();
        request.withStreamName(this.outputStreamName)
                .withRecords(currentRecords);

        PutRecordsResult result = sendRequest(request);
        if (result.getFailedRecordCount() > 0) {
            log.error(String.format("Failed to add %d records on shard id %s", result.getFailedRecordCount(), shardId));
        }

        try {
            checkpointer.checkpoint(lastRecord);
        } catch (InvalidStateException|ShutdownException e) {
            throw new RuntimeException(e);
        }

        this.recordCount = 0;
        this.byteCount = 0;
        this.currentRecords.clear();
    }

    PutRecordsResult sendRequest(PutRecordsRequest request) {
        return this.amazonKinesisClient.putRecords(request);
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        currentRecords.clear();
    }
}
