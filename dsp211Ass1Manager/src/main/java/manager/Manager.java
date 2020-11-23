package manager;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Manager {
    public static final String WOKERS_SQS = "TO_DO_QUEUE"; // sqs for workers
    public static final String WORKERS_TO_MANAGER_SQS = "COMPLETED_IMAGES_QUEUE"; // sqs for MANAGER to get messages from workers
    public static final String LOCALS_TO_MANAGER_SQS = "TASKS_FROM_LOCAL_QUEUE"; // sqs for MANAGER to get messages from locals
    public static final String IMAGE_URL = "imageUrl";
    public static final String INDEX = "index";
    public static final String TERMINATE = "terminate";
    public static final String LOCAL_ID = "localID";
    public static final String LOCAL_SQS_NAME = "localSqsName";
    public static final String S_3_BUCKET_NAME = "s3BucketName";
    public static final String S_3_BUCKET_KEY = "s3BucketKey";
    public static final Region REGION = Region.US_WEST_2;
    public static final String PARSED_TEXT = "parsedText";
    public static final String TEMP_FILE_PREFIX = "_tempFiles";

    private final int n;
    private int numberOfActiveWorkers = 0;
    private final String awsKey;
    private String queueWorkersUrl;
    private String queueWorkersToManagersUrl;
    private String queueLocalsToManagersUrl;
    private SqsClient sqs;
    private Map<String, Ec2Client> workersToEC2;
    private final Map<String, String> localQueues;
    private final Map<String, Integer> localToNumberOfTasksRemains;
    private final Map<String, Integer> localTempFileName;
    private final Map<String, Map<String, String>> localParsedImages;

    private boolean isTerminated;
    private S3Client s3;
    // download input file from s3
//    private final S3Client amazonS3Client = AmazonS3ClientBuilder.standard().build();

    public Manager(int n, String awsKey) {
        this.n = n;
        this.awsKey = awsKey;
        // hashmap to map keys of localId(string) to sqsQueues names(string)
        this.localQueues = new HashMap<>();
        // hashmap to map keys of localId(string) to number of tasks to complete for that local application(int)
        this.localToNumberOfTasksRemains = new HashMap<>();
        // hashmap to map keys of localId(string) to string array which: arr[0] = imageUrl, arr[1] = parsedText
        this.localParsedImages = new HashMap<>();
        this.localTempFileName = new HashMap<>();
        this.isTerminated = false;
    }

    public int getN() {
        return n;
    }

    public String getAwsKey() {
        return awsKey;
    }

    public void init() {

        //int numberOfImages = getNumberOfImages();
        // creating workers
        this.workersToEC2 = createWorkers(n);

        this.s3 = S3Client.builder().region(REGION).build();

        this.sqs = SqsClient.builder().region(REGION).build();
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(WOKERS_SQS)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (
                QueueNameExistsException e) {
            e.printStackTrace();

        }

        GetQueueUrlRequest getWorkersQueueRequest = GetQueueUrlRequest.builder()
                .queueName(WOKERS_SQS)
                .build();
        this.queueWorkersUrl = sqs.getQueueUrl(getWorkersQueueRequest).queueUrl();

        GetQueueUrlRequest getWorkersToManagerQueueRequest = GetQueueUrlRequest.builder()
                .queueName(WORKERS_TO_MANAGER_SQS)
                .build();
        this.queueWorkersToManagersUrl = sqs.getQueueUrl(getWorkersToManagerQueueRequest).queueUrl();

        GetQueueUrlRequest getLocalToManagerQueueRequest = GetQueueUrlRequest.builder()
                .queueName(LOCALS_TO_MANAGER_SQS)
                .build();
        this.queueLocalsToManagersUrl = sqs.getQueueUrl(getLocalToManagerQueueRequest).queueUrl();
    }

    public void run() {
        init();
        while (!this.isTerminated || !localToNumberOfTasksRemains.isEmpty()) {

            // check tasks from local applications
            // receive messages from the queue
            if (!this.isTerminated) {
                ReceiveMessageRequest receiveRequestsFromLocals = ReceiveMessageRequest.builder()
                        .queueUrl(queueLocalsToManagersUrl)
                        .build();
                List<Message> messagesFromLocalApplications = sqs.receiveMessage(receiveRequestsFromLocals).messages();
                for (Message m : messagesFromLocalApplications) {
                    // m = message from local application to manager
                    // localID, localSqsName, S3Bucket, S3BucketKey
                    if (this.isTerminated) {
                        break;
                    }
                    String localId = m.attributesAsStrings().get(LOCAL_ID);
                    String localSqsName = m.attributesAsStrings().get(LOCAL_SQS_NAME);
                    String s3BucketName = m.attributesAsStrings().get(S_3_BUCKET_NAME);
                    String s3BucketKey = m.attributesAsStrings().get(S_3_BUCKET_KEY);
                    //Integer N could be different from local to local
                    //String s3BucketFileName = m.attributesAsStrings().get("s3BucketFileName");

                    this.isTerminated = m.attributesAsStrings().get(TERMINATE).equals(TERMINATE);
                    this.localQueues.put(localId, localSqsName);
                    this.localTempFileName.put(localId, 1);

                    s3.getObject(GetObjectRequest.builder().bucket(s3BucketName).key(s3BucketKey).build(),
                            ResponseTransformer.toFile(Paths.get(localId + "_inputfile.txt")));
                    int currentLocalImagesCounter = 0;
                    try {
                        File currentInput = new File(Paths.get(localId + "_inputfile.txt").toUri());
                        BufferedReader br = new BufferedReader(new FileReader(currentInput));

                        String currentUrl;
                        while ((currentUrl = br.readLine()) != null) {
                            // adding current url to workers queue -> sending messages from manager to workers
                            Map<String, MessageAttributeValue> attr = new HashMap<>();
                            attr.put(LOCAL_ID, MessageAttributeValue.builder().stringValue(localId).build());
                            attr.put(IMAGE_URL, MessageAttributeValue.builder().stringValue(currentUrl).build());
                            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                                    .queueUrl(queueWorkersUrl)
                                    .messageBody(localId + " " + currentUrl)
                                    .messageAttributes(attr)
                                    .delaySeconds(5)
                                    .build();
                            sqs.sendMessage(send_msg_request);
                            // todo check if message arrives before increasing
                            currentLocalImagesCounter++;
                        }

                        // initilize workers:
                        int numberOfWorkersToAdd = (currentLocalImagesCounter / n) - numberOfActiveWorkers;
                        if (numberOfWorkersToAdd > 0) {
                            //add workers

                        }


                        String temp_files_bucket = localId + TEMP_FILE_PREFIX;

                        createBucket(temp_files_bucket, REGION);


                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    // save the amount of images to review to make sure we know when the task is done and html parsing can begin
                    this.localToNumberOfTasksRemains.put(localId, currentLocalImagesCounter);
                    // todo delete input file
                }
            }

            // after reviewing all messages from local -> check for finished tasks
            ReceiveMessageRequest receiveRequestsFromWorkers = ReceiveMessageRequest.builder()
                    .queueUrl(queueWorkersToManagersUrl)
                    .build();
            List<Message> messagesFromWorkers = sqs.receiveMessage(receiveRequestsFromWorkers).messages();
            for (Message m : messagesFromWorkers) {
                String localId = m.attributesAsStrings().get(LOCAL_ID);
                String imageUrl = m.attributesAsStrings().get(IMAGE_URL);
                String parsedText = m.attributesAsStrings().get(PARSED_TEXT);
                int temp = this.localToNumberOfTasksRemains.get(localId);
                this.localToNumberOfTasksRemains.put(localId, temp - 1);
                Map<String, String> currentLocalMap = this.localParsedImages.get(localId);
                currentLocalMap.put(imageUrl, parsedText);
                if (currentLocalMap.size() > 100) {
                    // size of map is getting to big -> upload all current data to s3 to merge later
                    StringBuilder builder = new StringBuilder();
                    for (Map.Entry<String, String> entry : currentLocalMap.entrySet()) {
                        builder.append(entry.getKey()).append(" ").append(entry.getValue()).append("\n");
                    }
                    // Put Object

                    s3.putObject(PutObjectRequest.builder().bucket(localId + TEMP_FILE_PREFIX).key(localId + TEMP_FILE_PREFIX + this.localTempFileName.get(localId))
                            .build(), RequestBody.fromString(builder.toString()));
                    int increment = this.localTempFileName.get(localId) + 1;
                    this.localTempFileName.put(localId, increment);
                }


            }
            // todo  check for done tasks -> send to html parser all the relevant data
            for (Map.Entry<String, Integer> entry : this.localToNumberOfTasksRemains.entrySet()) {
                if (entry.getValue() == 0) {
                    // task finished -> parse to html
                    //this.localParsedImages
                    // todo  check for done tasks -> send to html parser all the relevant data
                }
            }
        }

    }


    public Map<String, Ec2Client> createWorkers(int numberOfWorkersToAdd) {
        // todo
        return new HashMap<String, Ec2Client>();
    }

    private void createBucket(String bucket, Region region) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .locationConstraint(region.id())
                                .build())
                .build());

        System.out.println(bucket);
    }
}

