package manager;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sqs.model.UnsupportedOperationException;


import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager implements Runnable {
    public static final String WOKERS_SQS = "TO_DO_QUEUE"; // sqs for workers
    public static final String WORKERS_TO_MANAGER_SQS = "COMPLETED_IMAGES_QUEUE"; // sqs for MANAGER to get messages from workers
    public static final String LOCALS_TO_MANAGER_SQS = "TASKS_FROM_LOCAL_QUEUE"; // sqs for MANAGER to get messages from locals
    public static final String IMAGE_URL = "imageUrl";
    public static final String INDEX = "index";

    public static final String LOCAL_ID = "localID";
    public static final String NUM_OF_FILES = "numberOfFiles";
    public static final String LOCAL_SQS_NAME = "localSqsName";
    public static final String S_3_BUCKET_NAME = "s3BucketName";
    public static final String S_3_BUCKET_KEY = "s3BucketKey";
    public static final Region REGION = Region.US_EAST_1;
    public static final String PARSED_TEXT = "parsedText";
    public static final String TEMP_FILE_PREFIX = "tempfiles";
    public static final String TERMINATE = "Terminate";
    public static final String WORKER = "worker";
    public static final String HTML_FILE = "HTML_File";
    public static final String WORKER_AMI_ID = "ami-05a4f386f5050295f";
    public static final String WORKER_ARN = "arn:aws:iam::192532717092:instance-profile/Worker";
    public static final String N = "N";


    /**
     * number of active workers currently
     */
    private AtomicInteger numberOfActiveWorkers = new AtomicInteger(0);
    /**
     * the url of the workers to manager sqs queue
     */
    private String queueWorkersUrl;
    /**
     * the url of the workers to manager
     */
    private String queueWorkersToManagersUrl;
    /**
     * the url of the queue of the locals to manager
     */
    private String queueLocalsToManagersUrl;
    /**
     * sqs client object ot connect to all sqs queues
     */
    private SqsClient sqs;
    /**
     * list of id's of all EC2 computers running as workers
     */
    private List<String> workersEC2Ids;
    /**
     * map with localId keys to sqs queue that that local program listens to values
     */
    private final Map<String, String> localQueues;

    /**
     * map with localId keys to bucket name in s3 that that local program listens to values
     */
    private final Map<String, String> localBuckets;
    /**
     * map with localId keys to the number of tasks remains to parse values
     */
    private final Map<String, Integer> localToNumberOfTasksRemains;
    /**
     * map with localId keys to number of temp file save for that local program values
     */
    private final Map<String, Integer> localTempFileName;
    /**
     * map with localId keys to Map values of image url to Parsed text from that image
     */
    private final Map<String, Map<String, String>> localParsedImages;

    /**
     * boolean flag to determain if the manager should terminate or not
     */
    private AtomicBoolean isTerminated;
    /**
     * s3 object to upload and download files\object from s3 of AWS
     */
    private S3Client s3;


    public Manager() {

        // hashmap to map keys of localId(string) to sqsQueues names(string)
        this.localQueues = new ConcurrentHashMap<>();
        // hashmap to map keys of localId(string) to number of tasks to complete for that local application(int)
        this.localToNumberOfTasksRemains = new ConcurrentHashMap<>();
        // hashmap to map keys of localId(string) to string array which: arr[0] = imageUrl, arr[1] = parsedText
        this.localParsedImages = new ConcurrentHashMap<>();
        this.localTempFileName = new ConcurrentHashMap<>();
        this.localBuckets = new ConcurrentHashMap<>();
        this.isTerminated = new AtomicBoolean(false);
    }


    public void init() {

        //int numberOfImages = getNumberOfImages();
        // creating workers
        this.workersEC2Ids = new Vector<>();


        this.s3 = S3Client.builder().region(REGION).build();

        this.sqs = SqsClient.builder().region(REGION).build();
        try {
            CreateQueueRequest request_jobs = CreateQueueRequest.builder()
                    .queueName(WOKERS_SQS)
                    .build();
            CreateQueueResponse create_jobs_queue = sqs.createQueue(request_jobs);
            CreateQueueRequest request_results = CreateQueueRequest.builder()
                    .queueName(WORKERS_TO_MANAGER_SQS)
                    .build();
            CreateQueueResponse create_results_queue = sqs.createQueue(request_results);

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

    public void runWithoutThreads() {
        init();
        while (!isTerminated.get() || !localToNumberOfTasksRemains.isEmpty()) {
            if (!isTerminated.get()) {
                initLocalTasksWithoutThreads();
            }

            checkDoneTasksFromWorkersWithoutThreads();
            checkFinishedTasksToLocalsWithoutThreads();


        }
        terminateWorkers();
        removeSqss();
    }

    public void run() {
        init();
        // check tasks from local applications
        // receive messages from the queue
        Thread initLocalsThread = new Thread(new Runnable() {
            @Override
            public void run() {
                initLocalTask();
            }
        });
        Thread checkParsedFromWorkers = new Thread(new Runnable() {
            @Override
            public void run() {
                checkDoneTasksFromWorkers();
            }
        });

        Thread checkFinishedTasks = new Thread(new Runnable() {
            @Override
            public void run() {
                checkFinishedTasksToLocals();
            }
        });
        initLocalsThread.start();
        checkParsedFromWorkers.start();
        checkFinishedTasks.start();


        try {
            initLocalsThread.join();
            checkParsedFromWorkers.join();
            checkFinishedTasks.join();
        } catch (InterruptedException e) {
            System.out.println("Was Intruppted : " + e.getMessage());
            e.printStackTrace();

        }
        terminateWorkers();
        removeSqss();

    }

    private void
    checkFinishedTasksToLocals() {
        while (!isTerminated.get() || !localToNumberOfTasksRemains.isEmpty()) {
            checkFinishedTasksToLocalsWithoutThreads();
        }

    }

    private void checkFinishedTasksToLocalsWithoutThreads() {
        for (Map.Entry<String, Integer> entry : this.localToNumberOfTasksRemains.entrySet()) {
            if (entry.getValue() == 0) {
                //this.finishLocalAndCreateHTML(entry.getKey());
                String localId = entry.getKey();
                //adding message to sqs
                boolean success = false;
                while (!success) {

                    Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                    int numberOfFiles = localTempFileName.get(localId);
                    Map<String, String> currentLocalMap = this.localParsedImages.get(localId);
                    if (!currentLocalMap.isEmpty()) {
                        numberOfFiles++;
                        // size of map is getting too big -> upload all current data to s3 to merge later
                        int increment = this.localTempFileName.get(localId) + 1;
                        this.localTempFileName.put(localId, increment);
                        StringBuilder builder = new StringBuilder();
                        for (Map.Entry<String, String> currentParsedEntry : currentLocalMap.entrySet()) {
                            builder.append(currentParsedEntry.getKey()).append(((char) 5)).append(currentParsedEntry.getValue().replace("\n",Character.toString((char)6))).append("\n");
                        }
                        // Put Object
                        s3.putObject(PutObjectRequest.builder().bucket(this.localBuckets.get(localId)).key(localId + TEMP_FILE_PREFIX + numberOfFiles)
                                .build(), RequestBody.fromString(builder.toString()));
                    }
                    String numOfFilesInString = Integer.toString(numberOfFiles);
                    messageAttributes.put(NUM_OF_FILES, MessageAttributeValue.builder().dataType("String").stringValue(numOfFilesInString).build());
                    // messageAttributes.put(HTML_FILE, MessageAttributeValue.builder().dataType("String").stringValue(htmlParser.getFileName()).build());
                    SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                            .queueUrl(this.getQueueUrl(sqs, this.localQueues.get(localId)))
                            .messageAttributes(messageAttributes)
                            .messageBody("work is done")
                            .build();
                    SendMessageResponse res = sqs.sendMessage(sendMessageRequest);
                    if (res.sdkHttpResponse().isSuccessful()) {
                        success = true;
                    }

                }
                this.localToNumberOfTasksRemains.remove(localId);

            }
        }
    }

    private void checkDoneTasksFromWorkersWithoutThreads() {
        // after reviewing all messages from local -> check for finished tasks

        ReceiveMessageRequest receiveRequestsFromWorkers = ReceiveMessageRequest.builder()
                .queueUrl(queueWorkersToManagersUrl)
                .maxNumberOfMessages(10)
                .messageAttributeNames("All")
                .build();
        List<Message> messagesFromWorkers = sqs.receiveMessage(receiveRequestsFromWorkers).messages();
        for (Message m : messagesFromWorkers) {
            Map<String, MessageAttributeValue> attributes = m.messageAttributes();
            String localId = attributes.get(LOCAL_ID).stringValue();
            String imageUrl = attributes.get(IMAGE_URL).stringValue();
            String parsedText = attributes.get(PARSED_TEXT).stringValue();
            int temp = this.localToNumberOfTasksRemains.get(localId);
            this.localToNumberOfTasksRemains.put(localId, temp - 1);
            Map<String, String> currentLocalMap = this.localParsedImages.get(localId);
            currentLocalMap.put(imageUrl, parsedText);
            if (currentLocalMap.size() > 100) {
                // size of map is getting too big -> upload all current data to s3 to merge later
                int increment = this.localTempFileName.get(localId) + 1;
                this.localTempFileName.put(localId, increment);
                StringBuilder builder = new StringBuilder();
                for (Map.Entry<String, String> entry : currentLocalMap.entrySet()) {
                    builder.append(entry.getKey()).append(((char) 5)).append(entry.getValue().replace("\n",Character.toString((char)6))).append("\n");
                }
                // Put Object
                s3.putObject(PutObjectRequest.builder().bucket(this.localBuckets.get(localId)).key(localId + TEMP_FILE_PREFIX + this.localTempFileName.get(localId))
                        .build(), RequestBody.fromString(builder.toString()));
                this.localParsedImages.get(localId).clear();
            }
            //delete the message m from sqs
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueWorkersToManagersUrl)
                    .receiptHandle(m.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteRequest);
        }


    }


    private void checkDoneTasksFromWorkers() {
        // after reviewing all messages from local -> check for finished tasks
        while (!isTerminated.get() || !localToNumberOfTasksRemains.isEmpty()) {
            checkDoneTasksFromWorkersWithoutThreads();
        }

    }

    private void initLocalTask() {
        while (!this.isTerminated.get()) {
            initLocalTasksWithoutThreads();
        }
    }

    private void initLocalTasksWithoutThreads() {
        ReceiveMessageRequest receiveRequestsFromLocals = ReceiveMessageRequest.builder()
                .queueUrl(queueLocalsToManagersUrl)
                .maxNumberOfMessages(10)
                .messageAttributeNames("All")
                .build();
        List<Message> messagesFromLocalApplications = sqs.receiveMessage(receiveRequestsFromLocals).messages();
        for (Message m : messagesFromLocalApplications) {
            this.isTerminated.getAndSet(m.body().equals(TERMINATE));
            // m = message from local application to manager
            // localID, localSqsName, S3Bucket, S3BucketKey
            if (this.isTerminated.get()) {
                break;
            }
            Map<String, MessageAttributeValue> attributes = m.messageAttributes();
            String localId = attributes.get(LOCAL_ID).stringValue();
            String localSqsName = attributes.get(LOCAL_SQS_NAME).stringValue();
            String s3BucketName = attributes.get(S_3_BUCKET_NAME).stringValue();
            String s3BucketKey = attributes.get(S_3_BUCKET_KEY).stringValue();
            //System.out.println("localId->"+localId + " bucket key ->"+s3BucketKey+" s3BucketName ->"+s3BucketName +" N->"+m.attributesAsStrings().get(N) +" body->" +m.body());
            int currentN = Integer.parseInt(attributes.get(N).stringValue());
            //Integer N could be different from local to local
            //String s3BucketFileName = m.attributesAsStrings().get("s3BucketFileName");


            this.localQueues.put(localId, localSqsName);
            this.localTempFileName.put(localId, 0);
            this.localParsedImages.put(localId, new ConcurrentHashMap<>());
            this.localBuckets.put(localId, s3BucketName);

            s3.getObject(GetObjectRequest.builder().bucket(s3BucketName).key(s3BucketKey).build(),
                    ResponseTransformer.toFile(Paths.get(localId + "_inputfile.txt")));
            int currentLocalImagesCounter = 0;
            File currentInput = null;
            try {
                currentInput = new File(Paths.get(localId + "_inputfile.txt").toUri());
                BufferedReader br = new BufferedReader(new FileReader(currentInput));

                String currentUrl;
                while ((currentUrl = br.readLine()) != null) {
                    // adding current url to workers queue -> sending messages from manager to workers
                    Map<String, MessageAttributeValue> attr = new HashMap<>();
                    attr.put(LOCAL_ID, MessageAttributeValue.builder().dataType("String").stringValue(localId).build());
                    attr.put(IMAGE_URL, MessageAttributeValue.builder().dataType("String").stringValue(currentUrl).build());
                    boolean success = false;
                    while (!success)
                        try {
                            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                                    .queueUrl(queueWorkersUrl)
                                    .messageBody(localId + " " + currentUrl)
                                    .messageAttributes(attr)
                                    .delaySeconds(5)
                                    .build();
                            SendMessageResponse response = sqs.sendMessage(sendMsgRequest);
                            if (response.sdkHttpResponse().isSuccessful()) {
                                success = true;
                                currentLocalImagesCounter++;
                            }
                        } catch (InvalidMessageContentsException | UnsupportedOperationException e) {
                            System.out.println("Error found while sending the message: ");
                            e.printStackTrace();
                            success = false;
                        }

                }
                br.close();
                // initilize workers:
                float floatnum = ((float) currentLocalImagesCounter / (float) currentN) - numberOfActiveWorkers.get();
                int numberOfWorkersToAdd = (int) Math.ceil(floatnum);
                if (numberOfWorkersToAdd > 0) {
                    List<String> newWorkersList = this.createWorkers(numberOfWorkersToAdd);
                    this.workersEC2Ids.addAll(newWorkersList);
                    this.numberOfActiveWorkers.getAndSet(this.workersEC2Ids.size());

                }

            } catch (IOException e) {
                e.printStackTrace();
            }
            // save the amount of images to review to make sure we know when the task is done and html parsing can begin
            this.localToNumberOfTasksRemains.put(localId, currentLocalImagesCounter);
            if (currentInput != null && currentInput.delete()) {
                System.out.printf("finished sending tasks of %s_inputfile.txt\n", localId);
            } else {
                System.out.printf("something wrong when deleting file %s_inputfile.txt\n", localId);
            }
            //delete the message m from sqs
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueLocalsToManagersUrl)
                    .receiptHandle(m.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteRequest);
        }
    }

    /**
     * terminate every instance of worker
     */
    private void terminateWorkers() {
        Ec2Client ec2 = Ec2Client.builder().region(REGION).build();
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    for (Tag tag : instance.tags())
                        if (tag.value().equals(WORKER)) {
                            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                                    .instanceIds(instance.instanceId()).build();

                            ec2.terminateInstances(terminateRequest);
                            break;
                        }
                }
            }
            nextToken = response.nextToken();


        } while (nextToken != null);
        System.out.println("All workers are terminated");
    }

    /**
     * empty bucket and delete it from s3
     *
     * @param s3
     * @param bucket
     */
    public void emptyAndDeleteBucket(S3Client s3, String bucket) {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).build();
        ListObjectsV2Response listObjectsResponse;
        do {
            listObjectsResponse = s3.listObjectsV2(listObjectsV2Request);
            for (S3Object s3Object : listObjectsResponse.contents()) {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(s3Object.key()).build());
            }
            listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket)
                    .continuationToken(listObjectsResponse.nextContinuationToken())
                    .build();
        } while (listObjectsResponse.isTruncated());
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
        s3.deleteBucket(deleteBucketRequest);
        System.out.println("The bucket " + bucket + " has empty and deleted from S3");
    }

//    /**
//     * Creating final HTML file from all images url and parsed text, and upload it to the s3 AWS service for the local to find, and notify the matching local id
//     *
//     * @param localId id of the local application that asked for those images
//     */
//    private void finishLocalAndCreateHTML(String localId) {
//        System.out.println("Creating HTML File for " + localId);
//        HtmlParser htmlParser = new HtmlParser(localId);
//        boolean initResult = htmlParser.initFile();
//        if (!initResult) {
//            System.out.println("INIT HTML PARSING NOT WORKING!!!!!!!");
//        }
//        if (this.localTempFileName.get(localId) > 0) {
//            //there are temp files in s3 -> download each one and write to the html file
//            for (int i = 1; i < this.localTempFileName.get(localId); i++) {
//                String localBucketName = this.localQueues.get(localId);
//                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
//                        .bucket(localBucketName)
//                        .key(localId + TEMP_FILE_PREFIX + i)
//                        .build();
//
//
//                InputStream reader = s3.getObject(getObjectRequest, ResponseTransformer.toInputStream());
//                Scanner scanner = new Scanner(reader);
//                Map<String, String> parsed = new HashMap<>();
//                while (scanner.hasNext()) {
//                    String toAdd = scanner.nextLine();
//                    if (!toAdd.trim().isEmpty() && toAdd.contains(" ")) {
//                        String[] currentUrlAndParsed = scanner.nextLine().split(" ");
//                        parsed.put(currentUrlAndParsed[0], currentUrlAndParsed[1]);
//                    }
//                }
//                boolean result = htmlParser.appendListOfUrlAndTextToHTML(parsed);
//                if (!result) {
//                    System.out.println("HTML PARSING NOT WORKING!!!!!!!");
//                }
//            }
//        }
//        // write remaining parts in local parsed images
//        Map<String, String> lastToParse = this.localParsedImages.get(localId);
//        if (!lastToParse.isEmpty()) {
//            htmlParser.appendListOfUrlAndTextToHTML(lastToParse);
//        }
//        boolean endResult = htmlParser.endFile();
//        if (!endResult) {
//            System.out.println("END HTML PARSING NOT WORKING!!!!!!!");
//        }
//
//        //added file with s3
//        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
//                .bucket(this.localBuckets.get(localId))
//                .key(htmlParser.getFileName())
//                .build();
//        s3.putObject(putObjectRequest, Paths.get(htmlParser.getFileName()));
//
//        //delete local HTML
//        File LocalHTMLFileName = htmlParser.getHtmlFile();
//        if (LocalHTMLFileName != null && LocalHTMLFileName.delete()) {
//            System.out.printf("Local HTML %s is deleted \n", LocalHTMLFileName);
//        } else {
//            System.out.printf("something wrong when deleting file %s\n", LocalHTMLFileName);
//        }
//        //adding message to sqs
//        boolean success = false;
//        while (!success) {
//            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
//            messageAttributes.put(LOCAL_ID, MessageAttributeValue.builder().dataType("String").stringValue(localId).build());
//            messageAttributes.put(HTML_FILE, MessageAttributeValue.builder().dataType("String").stringValue(htmlParser.getFileName()).build());
//            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
//                    .queueUrl(this.getQueueUrl(sqs, this.localQueues.get(localId)))
//                    .messageAttributes(messageAttributes)
//                    .messageBody("html is done")
//                    .build();
//            SendMessageResponse res = sqs.sendMessage(sendMessageRequest);
//            if (res.sdkHttpResponse().isSuccessful()) {
//                success = true;
//            }
//
//        }
//        this.localToNumberOfTasksRemains.remove(localId);
//
//    }

    /**
     * get the url of a current queue
     *
     * @param sqsClient sqs cl;ient object to content the AWS SQS service
     * @param queueName name of the queue to search url for
     * @return url of the desired queue
     */
    public String getQueueUrl(SqsClient sqsClient, String queueName) {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();
    }


    /**
     * creating a single ec2Worker with worker jar
     *
     * @param ami_Id AMI id for the ec2
     * @param ec2    ec2 client object
     * @return id of the created ec2 worker
     */
    private String createEc2Instance(String ami_Id, Ec2Client ec2) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(ami_Id)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn(WORKER_ARN).build())
                .instanceType(InstanceType.T2_NANO)
                .maxCount(1)
                .minCount(1)
                .build();
        //todo- toAdd the proper jar with .userData(Base64.getEncoder().encodeToString("script based on download the file from s3 and java -jar path_to_jar".getBytes()))

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        Tag tag = Tag.builder()
                .key("Name")
                .value(Manager.WORKER)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();
        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 Instance %s based on AMI %s\n",
                    instanceId, ami_Id);
            // System.out.println("Successfully started EC2 Instance " + instanceId + " based on AMI " + ami_Id);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return instanceId;
    }


    /**
     * Creating given number of EC2 workers
     *
     * @param numberOfWorkersToAdd number of workers to create
     * @return list of workers id's
     */
    private List<String> createWorkers(int numberOfWorkersToAdd) {
        Ec2Client ec2 = Ec2Client.builder().region(REGION).build();
        List<String> ec2Ids = new Vector<>();

        for (int i = 1; i <= numberOfWorkersToAdd; i++) {
            System.out.printf("Creating worker %d\n", i);
            String currentId = createEc2Instance(WORKER_AMI_ID, ec2);
            ec2Ids.add(currentId);
            System.out.printf("worker %d  is Running\n", i);
        }

        return ec2Ids;
    }

    private void removeSqss() {
        deleteQueue(queueWorkersUrl);
        deleteQueue(queueWorkersToManagersUrl);
        deleteQueue(queueLocalsToManagersUrl);
    }

    private void deleteQueue(String queueNameUrl) {
        try {
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueNameUrl)
                    .build();

            this.sqs.deleteQueue(deleteQueueRequest);

        } catch (QueueNameExistsException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


//    /**
//     * Creating bucket in s3 in AWS
//     *
//     * @param bucket name of the bucket
//     */
//    private void createBucket(String bucket) {
//        s3.createBucket(CreateBucketRequest
//                .builder()
//                .bucket(bucket)
////                .createBucketConfiguration(
////                        CreateBucketConfiguration.builder()
////                                .locationConstraint(REGION.id())
////                                .build())
//                .build());
//
//        System.out.println(bucket);
//    }
}

