package manager;

//region imports

import javafx.util.Pair;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
//endregion imports

/**
 * Manager Class to run the main Manager Task:
 * 1. Listen to local application registration messages, create workers if necessary , and put the in the "to-do" queue for the workers
 * 2. Accept completed tasks from workers , and keep count of how many images left from each registered local application
 * 3. When local application finished, upload rest of images remaining in ram to s3 and inform the matching local app that the task is done.
 * Repeat those 3 steps until termination flag arrive and all remaining tasks are done
 */
public class Manager implements Runnable {
    /**
     * String: name of the queue to send tasks that remained to do for workers
     */
    public static final String WORKERS_SQS = "TO_DO_QUEUE";
    /**
     * String : name of the queue to send completed tasks from workers to manager
     */
    public static final String WORKERS_TO_MANAGER_SQS = "COMPLETED_IMAGES_QUEUE";
    /**
     * String: name of the registration queue from loacl app to manager
     */
    public static final String LOCALS_TO_MANAGER_SQS = "TASKS_FROM_LOCAL_QUEUE";
    /**
     * String: sqs message attribute for image url
     */
    public static final String IMAGE_URL = "imageUrl";
    /**
     * String: sqs message attribute for id of a local application
     */
    public static final String LOCAL_ID = "localID";
    /**
     * String: sqs message attribute for number of files
     */
    public static final String NUM_OF_FILES = "numberOfFiles";

    /**
     * String: sqs message attribute of local app Sqs queue name
     */
    public static final String LOCAL_SQS_NAME = "localSqsName";
    /**
     * String: sqs attribute of bucket name of a registering local app
     */
    public static final String S_3_BUCKET_NAME = "s3BucketName";
    /**
     * String: sqs attribute of bucket object key of a registering local app
     */
    public static final String S_3_BUCKET_KEY = "s3BucketKey";
    /**
     * Region Constant for AWS instances
     */
    public static final Region REGION = Region.US_EAST_1;
    /**
     * String: sqs message attribute for the parsed text coming from worker
     */
    public static final String PARSED_TEXT = "parsedText";
    /**
     * String: constant prefix for temp file to upload to s3
     */
    public static final String TEMP_FILE_PREFIX = "tempfiles";
    /**
     * String: sqs attributes for sqs messages for terminate command from locals
     */
    public static final String TERMINATE = "Terminate";
    /**
     * String: constant for the worker tag
     */
    public static final String WORKER = "worker";
    /**
     * String : sqs message attributes to determine how many urls were sent from the local appication
     */
    public static final String NUMBER_OF_URLS = "numberOfUrls";
    /**
     * String: worker image id to create workers with (ami id)
     */
    public static final String WORKER_AMI_ID = "ami-05a4f386f5050295f";
    /**
     * String: Worker ARN constant
     */
    public static final String WORKER_ARN = "arn:aws:iam::192532717092:instance-profile/Worker";
    /**
     * String : sqs message attribute of  number of images per worker
     */
    public static final String N = "N";
    /**
     * String: bash command to initialize the worker instances with
     */
    public static final String USER_DATA_WORKER =
            "#!/bin/bash\n" +
                    "sudo mkdir /home/ass/\n" +
                    "sudo aws s3 cp s3://bucketforjar/Worker-jar.jar /home/ass/\n" +
                    "sudo /usr/bin/java -jar -Xmx1g /home/ass/Worker-jar.jar\n";
    /**
     * Integer: Max number of images to save for on local memory for each local application
     */
    public static final int MAX_IMG_PER_LOCAL = 100;
    /**
     * value of delimiter between url to the parsed task from it in the files that are uploaded to s3 for the local application to download from
     */
    public static final int DELIMITER_BETWEEN_URL_TO_PARSE = 5;
    /**
     * value of delimiter between two images in the summary file in s3
     */
    public static final int DELIMITER_BETWEEN_PARSINGS = 6;
    /**
     * ReadWriteLock to synchronized data between inner threads properly
     */
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);


    /**
     * Integer Number of active workers currently
     */
    private AtomicInteger numberOfActiveWorkers = new AtomicInteger(0);
    /**
     * String: The url of the workers to manager sqs queue
     */
    private String queueWorkersUrl;
    /**
     * String: The url of the workers to manager
     */
    private String queueWorkersToManagersUrl;
    /**
     * String: The url of the queue of the locals to manager
     */
    private String queueLocalsToManagersUrl;
    /**
     * SQS Client object ot connect to all sqs queues
     */
    private SqsClient sqs;
    /**
     * List of id's of all EC2 computers running as workers
     */
    private List<String> workersEC2Ids;
    /**
     * Map with localId keys to sqs queue that that local program listens to values
     */
    private final Map<String, String> localQueues;

    /**
     * Map with localId keys to bucket name in s3 that that local program listens to values
     */
    private final Map<String, String> localBuckets;
    /**
     * Map with localId keys to the number of tasks remains to parse values
     */
    private final Map<String, AtomicInteger> localToNumberOfTasksRemains;
    /**
     * Map with localId keys to number of temp file save for that local program values
     */
    private final Map<String, AtomicInteger> localTempFileName;
    /**
     * Map with localId keys to Map values of image url to Parsed text from that image
     */
    private final Map<String, Pair<StringBuffer, AtomicInteger>> localParsedImages;

    /**
     * Boolean flag to determine if the manager should terminate or not
     */
    private AtomicBoolean isTerminated;

    /**
     * S3 object to upload and download files\object from s3 of AWS
     */
    private S3Client s3;


    /**
     * constructor for the manager application
     */
    public Manager() {
        this.localQueues = new ConcurrentHashMap<>();
        this.localToNumberOfTasksRemains = new ConcurrentHashMap<>();
        this.localParsedImages = new ConcurrentHashMap<>();
        this.localTempFileName = new ConcurrentHashMap<>();
        this.localBuckets = new ConcurrentHashMap<>();
        this.isTerminated = new AtomicBoolean(false);
    }


    /**
     * function to initialize all data structures and AWS instances for the communication
     */
    public void init() {

        //int numberOfImages = getNumberOfImages();
        // creating workers
        this.workersEC2Ids = new Vector<>();


        this.s3 = S3Client.builder().region(REGION).build();

        this.sqs = SqsClient.builder().region(REGION).build();
        try {
            CreateQueueRequest request_jobs = CreateQueueRequest.builder()
                    .queueName(WORKERS_SQS)
                    .build();
            CreateQueueResponse createJobsQueue = sqs.createQueue(request_jobs);
            CreateQueueRequest request_results = CreateQueueRequest.builder()
                    .queueName(WORKERS_TO_MANAGER_SQS)
                    .build();
            CreateQueueResponse createQueueResponse = sqs.createQueue(request_results);

        } catch (
                QueueNameExistsException e) {
            e.printStackTrace();

        }

        GetQueueUrlRequest getWorkersQueueRequest = GetQueueUrlRequest.builder()
                .queueName(WORKERS_SQS)
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

    /**
     * Run all manager tasks in a single thread in order,until termination message arrives and all tasks are finished
     */
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

    /**
     * main Manager task:
     * 1. Listen to local application registration messages, create workers if necessary , and put the in the "to-do" queue for the workers
     * 2. Accept completed tasks from workers , and keep count of how many images left from each registered local application
     * 3. When local application finished, upload rest of images remaining in ram to s3 and inform the matching local app that the task is done.
     * Repeat those 3 steps until termination flag arrive and all remaining tasks are done
     */
    public void run() {
        // initializing all data requred for the manager to run
        init();

        // define all threads for the manager tasks
        Thread initLocalsThread = new Thread(this::initLocalTask);
        Thread checkParsedFromWorkers = new Thread(this::checkDoneTasksFromWorkers);
        Thread checkFinishedTasks = new Thread(this::checkFinishedTasksToLocals);

        //start all threads
        initLocalsThread.start();
        checkParsedFromWorkers.start();
        checkFinishedTasks.start();


        try {
            // wait for all the threads to finish
            initLocalsThread.join();
            checkParsedFromWorkers.join();
            checkFinishedTasks.join();
        } catch (InterruptedException e) {
            System.out.println("Was Intruppted : " + e.getMessage());
            e.printStackTrace();

        }
        // terminating workers and deleting all remaining queues
        terminateWorkers();
        removeSqss();

    }


    /**
     * main loop for the 1st thread of the manager app
     */
    private void initLocalTask() {
        while (!this.isTerminated.get()) {
            initLocalTasksWithoutThreads();
        }
    }

    /**
     * main task of the 1st thread of the manager : listen to new local application that want to register to the services of the manager.
     * receive all parameters needed to save data and parse it for the current local application, and send all urls to parsing
     */
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
            int numberOfUrls = Integer.parseInt(attributes.get(NUMBER_OF_URLS).stringValue());
            //System.out.println("localId->"+localId + " bucket key ->"+s3BucketKey+" s3BucketName ->"+s3BucketName +" N->"+m.attributesAsStrings().get(N) +" body->" +m.body());
            int currentN = Integer.parseInt(attributes.get(N).stringValue());
            //Integer N could be different from local to local
            //String s3BucketFileName = m.attributesAsStrings().get("s3BucketFileName");
            readWriteLock.writeLock().lock();
            this.localToNumberOfTasksRemains.put(localId, new AtomicInteger(numberOfUrls));

            this.localQueues.put(localId, localSqsName);
            this.localTempFileName.put(localId, new AtomicInteger(0));
            this.localParsedImages.put(localId, new Pair<>(new StringBuffer(), new AtomicInteger(0)));
            this.localBuckets.put(localId, s3BucketName);
            readWriteLock.writeLock().unlock();
            s3.getObject(GetObjectRequest.builder().bucket(s3BucketName).key(s3BucketKey).build(),
                    ResponseTransformer.toFile(Paths.get(localId + "_inputfile.txt")));
            //int currentLocalImagesCounter = 0;
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
                                //currentLocalImagesCounter++;
                            }
                        } catch (InvalidMessageContentsException | UnsupportedOperationException e) {
                            System.out.println("Error found while sending the message: ");
                            e.printStackTrace();
                            success = false;
                        }

                }
                br.close();
                // save the amount of images to review to make sure we know when the task is done and html parsing can begin

                // initilize workers:
                float floatnum = ((float) numberOfUrls / (float) currentN) - numberOfActiveWorkers.get();
                int numberOfWorkersToAdd = (int) Math.ceil(floatnum);
                if (numberOfWorkersToAdd > 0) {
                    List<String> newWorkersList = this.createWorkers(numberOfWorkersToAdd);
                    this.workersEC2Ids.addAll(newWorkersList);
                    this.numberOfActiveWorkers.getAndSet(this.workersEC2Ids.size());
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
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
     * main loop for the 2nd thread task of the manager app
     */
    private void checkDoneTasksFromWorkers() {
        // after reviewing all messages from local -> check for finished tasks
        while (!isTerminated.get() || !localToNumberOfTasksRemains.isEmpty()) {
            checkDoneTasksFromWorkersWithoutThreads();
        }

    }

    /**
     * 2nd main task for the manager- check for parsed images from workers, save parse data to the url,
     * and match the result with the matching local application
     */
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

            readWriteLock.writeLock().lock();
            StringBuffer stringBuilder = this.localParsedImages.get(localId).getKey();
            AtomicInteger amountOfImages = this.localParsedImages.get(localId).getValue();
            stringBuilder.append(imageUrl).append(((char) DELIMITER_BETWEEN_URL_TO_PARSE)).append(parsedText.replace("\n", Character.toString((char) DELIMITER_BETWEEN_PARSINGS))).append("\n");
            amountOfImages.getAndIncrement();
            System.out.println("added image: " + imageUrl);
            if (amountOfImages.get() > MAX_IMG_PER_LOCAL) {
                // size of map is getting too big -> upload all current data to s3 to merge later
                // Put Object
                System.out.println("reached 100 limit for " + localId + ". buffer is: \n" + stringBuilder.toString());
                s3.putObject(PutObjectRequest.builder().bucket(this.localBuckets.get(localId)).key(localId + TEMP_FILE_PREFIX + this.localTempFileName.get(localId))
                        .build(), RequestBody.fromString(stringBuilder.toString()));
                this.localTempFileName.get(localId).getAndIncrement();
                this.localParsedImages.put(localId, new Pair<>(new StringBuffer(), new AtomicInteger(0)));

            }


            //delete the message m from sqs
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueWorkersToManagersUrl)
                    .receiptHandle(m.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteRequest);
            this.localToNumberOfTasksRemains.get(localId).getAndDecrement();
            System.out.println("tasks remained for " + localId + ": " + this.localToNumberOfTasksRemains.get(localId));
            readWriteLock.writeLock().unlock();
        }


    }

    /**
     * Main task loop for the 3rd thread : check for finished tasks from local applications
     */
    private void
    checkFinishedTasksToLocals() {
        while (!isTerminated.get() || !localToNumberOfTasksRemains.isEmpty()) {
            checkFinishedTasksToLocalsWithoutThreads();
        }

    }

    /**
     * 3rd task of the manager: check for finished tasks from local application to send to upload all remaining data to s3
     * and notify the local application it's task is done and it can convert it to html now
     */
    private void checkFinishedTasksToLocalsWithoutThreads() {
        readWriteLock.readLock().lock();

        Map<String, AtomicInteger> doneTasks = this.localToNumberOfTasksRemains.entrySet()
                .stream()
                .filter(entry -> entry.getValue().get() == 0)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        readWriteLock.readLock().unlock();

        for (Map.Entry<String, AtomicInteger> entry : doneTasks.entrySet()) {
            String localId = entry.getKey();
            //adding message to sqs
            boolean success = false;
            while (!success) {

                Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                //int numberOfFiles = localTempFileName.get(localId);
                StringBuffer stringBuilder = this.localParsedImages.get(localId).getKey();
                if (!stringBuilder.toString().isEmpty()) {
                    // size of map is getting too big -> upload all current data to s3 to merge later
                    // Put Object
                    s3.putObject(PutObjectRequest.builder().bucket(this.localBuckets.get(localId)).key(localId + TEMP_FILE_PREFIX + localTempFileName.get(localId))
                            .build(), RequestBody.fromString(stringBuilder.toString()));
                    this.readWriteLock.writeLock().lock();
                    this.localTempFileName.get(localId).getAndIncrement();
                    this.readWriteLock.writeLock().unlock();


                }
                String numOfFilesInString = Integer.toString(localTempFileName.get(localId).get());
                messageAttributes.put(NUM_OF_FILES, MessageAttributeValue.builder().dataType("String").stringValue(numOfFilesInString).build());
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

            // clearing all data of that local application from manager
            readWriteLock.writeLock().lock();
            this.localToNumberOfTasksRemains.remove(localId);
            this.localTempFileName.remove(localId);
            this.localQueues.remove(localId);
            this.localBuckets.remove(localId);
            this.localParsedImages.remove(localId);
            readWriteLock.writeLock().unlock();
        }

    }


    /**
     * Terminates all remaining workers instances
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
     * Empty bucket and delete it from s3
     *
     * @param s3     S3Client object to send request with
     * @param bucket String: bucket to delete
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


    /**
     * Get the url of a current queue
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
     * Creating a single ec2Worker with worker jar
     *
     * @param amiId AMI id for the ec2
     * @param ec2   ec2 client object
     * @return id of the created ec2 worker
     */
    private String createEc2Instance(String amiId, Ec2Client ec2) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn(WORKER_ARN).build())
                .instanceType(InstanceType.T2_MICRO)
                .userData(Base64.getEncoder().encodeToString(USER_DATA_WORKER.getBytes()))
                .maxCount(1)
                .minCount(1)
                .build();

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
                    instanceId, amiId);
            // System.out.println("Successfully started EC2 Instance " + instanceId + " based on AMI " + amiId);

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

    /**
     * Remove all remaining SQS queues from AWS services
     */
    private void removeSqss() {
        deleteQueue(queueWorkersUrl);
        deleteQueue(queueWorkersToManagersUrl);
        deleteQueue(queueLocalsToManagersUrl);
    }

    /**
     * Delete a SQS Queue according ot the given Url
     *
     * @param queueNameUrl String: name of the url to delete
     */
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
}

