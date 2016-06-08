package er.replicator;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.*;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class RecordProcessor implements IRecordProcessor {

    public static final Log log = LogFactory.getLog(RecordProcessor.class);

    // we'll cap our requests (kinesis supports up to 5mb per request)
    private static final int MAX_BYTES = 4 * 1024 * 1024;

    private final String outputStreamName;
    private final AmazonKinesisClient amazonKinesisClient;
    private final List<PutRecordsRequestEntry> currentRecords = Lists.newLinkedList();

    private String shardId;
    private int recordCount;
    private int byteCount;

    private int maxRecords;


    public RecordProcessor(String outputStreamName, AmazonKinesisClient client, int maxPutRecords) {
        this.outputStreamName = outputStreamName;
        this.amazonKinesisClient = client;
        this.maxRecords = maxPutRecords;
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
        return ((recordCount >= maxRecords) || (byteCount >= MAX_BYTES));
    }

    /**
     * Checkpoint hook.  Prior to checkpointing, we'll send all collected records to the shard.
     *
     * @param checkpointer
     * @param lastRecord
     */
    void checkpoint(IRecordProcessorCheckpointer checkpointer, Record lastRecord) {

        log.debug(String.format("Attempting to checkpoint shard %s %d records %d bytes", shardId, recordCount, byteCount));
        PutRecordsRequest request = new PutRecordsRequest();
        request.withStreamName(this.outputStreamName)
                .withRecords(currentRecords);

        sendRequestAndRetryFailedRecords(request);

        try {
            checkpointer.checkpoint(lastRecord);
        } catch (InvalidStateException|ShutdownException e) {
            throw new RuntimeException(e);
        }

        log.debug("Successfully checkpointed shard " + shardId);

        this.recordCount = 0;
        this.byteCount = 0;
        this.currentRecords.clear();
    }

    /**
     * Send the request and infinitely retry any records on failure (using exponential backoff).
     *
     * @param request The request to send to kinesis.
     */
    void sendRequestAndRetryFailedRecords(PutRecordsRequest request) {

        PutRecordsRequest currentRequest = request;
        PutRecordsResult result = sendRequestWithInfiniteRetries(request);

        long errorCount = 0;

        while (result.getFailedRecordCount() > 0) {
            errorCount += 1;

            log.error(String.format("PutRecords failed for %d of %d records, shard %s.  Will retry these records.",
                    result.getFailedRecordCount(), result.getRecords().size(), shardId));

            List<PutRecordsRequestEntry> requestEntries = currentRequest.getRecords();
            List<PutRecordsResultEntry> resultEntries = result.getRecords();

            // # of records in the output should match the number in the input
            if (requestEntries.size() != resultEntries.size()) {
                throw new RuntimeException("Kinesis assert failed, number of output records != input records");
            }

            List<PutRecordsRequestEntry> retryEntries = Lists.newLinkedList();

            // zip functionality would be nice
            for (int i = 0; i < currentRequest.getRecords().size(); i++) {
                PutRecordsRequestEntry requestEntry = requestEntries.get(i);
                PutRecordsResultEntry resultEntry = resultEntries.get(i);

                if (resultEntry.getErrorCode() != null) {
                    retryEntries.add(requestEntry);
                }
            }


            if (retryEntries.isEmpty()) {
                result.setFailedRecordCount(0);
            } else {
                currentRequest = new PutRecordsRequest();
                currentRequest.withStreamName(outputStreamName)
                        .withRecords(retryEntries);

                long delayMillis = (long) Math.min(Math.pow(2, errorCount - 1) * 100, 6400);
                try {
                    log.debug(String.format("Sleeping for %dms on shard %s", delayMillis, shardId));
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                log.debug(String.format("Retrying request with %d records", currentRequest.getRecords().size()));

                result = sendRequestWithInfiniteRetries(currentRequest);
            }
        }

    }


    /**
     * Sends the request to the output kinesis stream with infinite retires in the case of a kinesis failure.
     *
     * @param request The request to send.
     * @return The result after a successful call.
     */
    PutRecordsResult sendRequestWithInfiniteRetries(PutRecordsRequest request) {
        int errorCount = 0;
        while (true) {
            try {
                return this.amazonKinesisClient.putRecords(request);
            } catch (Exception e) {
                errorCount += 1;
                long delayMillis = (long) Math.min(Math.pow(2, errorCount - 1) * 100, 6400);
                try {
                    log.debug(String.format("Sleeping for %dms on shard %s", delayMillis, shardId));
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e2) {
                    throw new RuntimeException(e2);
                }


                log.error(String.format("Error sending data to kinesis, retrying in %dms", delayMillis), e);
            }
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        currentRecords.clear();
    }
}
