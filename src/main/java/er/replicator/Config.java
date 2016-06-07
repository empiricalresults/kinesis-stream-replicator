package er.replicator;

import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.constraints.NotNull;

@ConfigurationProperties
public class Config {

    @NotNull
    private String appName;

    @NotNull
    public String kinesisInputStream;

    @NotNull
    public String kinesisOutputStream;

    // max records returned per kinesis get request, default is 10k in the kcl client
    private int maxKinesisGetRecords = 10000;

    private String initialPositionInStream = "LATEST";

    private String inputStreamAwsRegion = "us-east-1";
    private String outputStreamAwsRegion = "us-east-1";

    private int dynamoReadCapacity = 1;
    private int dynamoWriteCapacity = 1;

    private String inputStreamCredentialsProvider;
    private String outputStreamCredentialsProvider;
    private String dynamoCredentialsProvider;
    private String cloudwatchCredentialsProvider;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }


    public int getMaxKinesisGetRecords() {
        return maxKinesisGetRecords;
    }

    public void setMaxKinesisGetRecords(int maxKinesisGetRecords) {
        this.maxKinesisGetRecords = maxKinesisGetRecords;
    }

    public String getInitialPositionInStream() {
        return initialPositionInStream;
    }

    public void setInitialPositionInStream(String initialPositionInStream) {
        this.initialPositionInStream = initialPositionInStream;
    }

    public String getInputStreamAwsRegion() {
        return inputStreamAwsRegion;
    }

    public void setInputStreamAwsRegion(String inputStreamAwsRegion) {
        this.inputStreamAwsRegion = inputStreamAwsRegion;
    }

    public String getOutputStreamAwsRegion() {
        return outputStreamAwsRegion;
    }

    public void setOutputStreamAwsRegion(String outputStreamAwsRegion) {
        this.outputStreamAwsRegion = outputStreamAwsRegion;
    }

    public int getDynamoReadCapacity() {
        return dynamoReadCapacity;
    }

    public void setDynamoReadCapacity(int dynamoReadCapacity) {
        this.dynamoReadCapacity = dynamoReadCapacity;
    }

    public int getDynamoWriteCapacity() {
        return dynamoWriteCapacity;
    }

    public void setDynamoWriteCapacity(int dynamoWriteCapacity) {
        this.dynamoWriteCapacity = dynamoWriteCapacity;
    }

    public String getInputStreamCredentialsProvider() {
        return inputStreamCredentialsProvider;
    }

    public void setInputStreamCredentialsProvider(String inputStreamCredentialsProvider) {
        this.inputStreamCredentialsProvider = inputStreamCredentialsProvider;
    }

    public String getOutputStreamCredentialsProvider() {
        return outputStreamCredentialsProvider;
    }

    public void setOutputStreamCredentialsProvider(String outputStreamCredentialsProvider) {
        this.outputStreamCredentialsProvider = outputStreamCredentialsProvider;
    }

    public String getDynamoCredentialsProvider() {
        return dynamoCredentialsProvider;
    }

    public void setDynamoCredentialsProvider(String dynamoCredentialsProvider) {
        this.dynamoCredentialsProvider = dynamoCredentialsProvider;
    }

    public String getCloudwatchCredentialsProvider() {
        return cloudwatchCredentialsProvider;
    }

    public void setCloudwatchCredentialsProvider(String cloudwatchCredentialsProvider) {
        this.cloudwatchCredentialsProvider = cloudwatchCredentialsProvider;
    }

    public String getKinesisInputStream() {
        return kinesisInputStream;
    }

    public void setKinesisInputStream(String kinesisInputStream) {
        this.kinesisInputStream = kinesisInputStream;
    }

    public String getKinesisOutputStream() {
        return kinesisOutputStream;
    }

    public void setKinesisOutputStream(String kinesisOutputStream) {
        this.kinesisOutputStream = kinesisOutputStream;
    }
}
