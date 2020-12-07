package localApplication;


import javafx.util.Pair;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


import java.io.*;
import java.nio.file.Paths;
import java.util.*;


public class LocalApplication {

    public final static int INPUT_FILE_NAME_INDEX = 0;
    public final static int OUTPUT_FILE_NAME_INDEX = 1;
    public final static int N_INDEX = 2;
    public final static int TERMINATE_INDEX = 3;
    public final static Region REGION = Region.US_EAST_1;
    public static final String NUM_OF_FILES = "numberOfFiles";
    public static final String MANAGER = "manager";
    public static final String LOCALS_TO_MANAGER_SQS = "TASKS_FROM_LOCAL_QUEUE";
    public static final String LOCAL_ID = "localID";
    public static final String LOCAL_SQS_NAME = "localSqsName";
    public static final String S_3_BUCKET_NAME = "s3BucketName";
    public static final String S_3_BUCKET_KEY = "s3BucketKey";
    public static final String AMI_ID = "ami-06af1f9a5f7fe2e06";
    public static final String MANAGER_ARN = "arn:aws:iam::192532717092:instance-profile/Manager-role";
    public static final String TERMINATE = "Terminate";
    public static final String HTML_FILE = "HTML_File";
    public static final String NUMBER_OF_URLS = "numberOfUrls";
    public static final String TEMP_FILE_PREFIX = "tempfiles";
    public static final String USER_DATA_MANAGER = "";
//            "#!/bin/bash\n" +
//                    "sudo mkdir /home/ass/\n" +
//                    "sudo aws s3 cp s3://bucketforjar/Manager.jar /home/ass/\n" +
//                    "sudo /usr/bin/java -jar /home/ass/Manager.jar\n" +
//                    "shutdown -h now";


    private String inputFileName;
    private String outputFileName;
    private int N;
    private boolean isTerminate;


    public LocalApplication(String[] args) {
        this.inputFileName = args[INPUT_FILE_NAME_INDEX];
        this.outputFileName = args[OUTPUT_FILE_NAME_INDEX];
        this.N = Integer.parseInt(args[N_INDEX]);
        this.isTerminate = (args.length > 3 && args[TERMINATE_INDEX].equals("terminate"));
    }

    public String getInputFileName() {
        return inputFileName;
    }

    public String getOutputFileName() {
        return outputFileName;
    }


    private int countNumberOfUrls(String filename) {
        int counter = 0;
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String currentUrl;
            while ((currentUrl = br.readLine()) != null) {
                if (currentUrl.trim().isEmpty()) {
                    continue;
                } else {
                    counter++;
                }

            }
            return counter;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

    }


    public void run() throws Exception {

        System.out.println("ALL GOOD...");
        String app_name = "LocalApp" + System.currentTimeMillis();
        String queue_name = app_name + "Queue";
        String numberOfUrls = Integer.toString(countNumberOfUrls(this.inputFileName + ".txt"));
        final String bucket = "bucket" + System.currentTimeMillis();
        Ec2Client ec2 = Ec2Client.builder().region(REGION).build();
        SqsClient sqs_client = SqsClient.builder().region(REGION).build();
        S3Client s3 = S3Client.builder().region(REGION).build();
        createManagerIfNotRunning(ec2, sqs_client);
        String file_name = uploadFileToS3(s3, bucket);
        createLocalQueue(sqs_client, queue_name);
        // m = message from local application to manager
        sendRegistrationMessage(sqs_client, app_name, queue_name, bucket, file_name, numberOfUrls);

        boolean receive_ans = false;
        System.out.println("Waiting for result from manager...");
        while (!receive_ans) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(this.getQueueUrl(sqs_client, queue_name))
                    .waitTimeSeconds(10)
                    .visibilityTimeout(1)
                    .messageAttributeNames("All")
                    .build();
            List<Message> messages = sqs_client.receiveMessage(receiveMessageRequest).messages();
            for (Message m : messages) {
                if (m.body().startsWith("work is done")) {
                    receive_ans = true;
                    Map<String, MessageAttributeValue> attributes = m.messageAttributes();

                    int numberOfFiles = Integer.parseInt(attributes.get(NUM_OF_FILES).stringValue());
                    HtmlParser htmlParser = new HtmlParser(outputFileName);
                    boolean initResult = htmlParser.initFile();
                    if (!initResult) {
                        System.out.println("INIT HTML PARSING NOT WORKING!!!!!!!");
                    }
                    for (int i = 0; i < numberOfFiles; i++) {

                        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                .bucket(bucket)
                                .key(app_name + TEMP_FILE_PREFIX + i)
                                .build();
                        System.out.println("key is: " + app_name + TEMP_FILE_PREFIX + i);

                        InputStream reader = s3.getObject(getObjectRequest, ResponseTransformer.toInputStream());
                        Scanner scanner = new Scanner(reader);
                        List<Pair<String, String>> parsed = new Vector<>();
                        while (scanner.hasNext()) {
                            String toAdd = scanner.nextLine();
                            if (!toAdd.trim().isEmpty() && toAdd.contains(Character.toString((char) 5))) {
                                String[] currentUrlAndParsed = toAdd.split(Character.toString((char) 5));
                                parsed.add(new Pair<>(currentUrlAndParsed[0], currentUrlAndParsed[1].replace(Character.toString((char) 6), "\n")));
                            }
                        }
                        boolean result = htmlParser.appendListOfUrlAndTextToHTML(parsed);
                        if (!result) {
                            System.out.println("HTML PARSING NOT WORKING!!!!!!!");
                        }
                    }
                    boolean result = htmlParser.endFile();
                    if (result) {
                        System.out.println("HTML file is parsed and ready");
                    }

                    //s3.getObject(GetObjectRequest.builder().bucket(bucket).key(HTML_file).build(), ResponseTransformer.toFile(Paths.get(this.getOutputFileName() + ".html")));
                    emptyAndDeleteBucket(s3, bucket);
                    deleteQueue(sqs_client, queue_name);

                    break;
                }
            }
        }
        if (this.isTerminate) {
            System.out.println("Sending termination message");
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(this.getQueueUrl(sqs_client, LOCALS_TO_MANAGER_SQS))
                    .messageBody(TERMINATE)
                    .build();
            sqs_client.sendMessage(sendMessageRequest);
        }

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

    private void deleteQueue(SqsClient sqsClient, String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqsClient.deleteQueue(deleteQueueRequest);

        } catch (QueueNameExistsException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private String getQueueUrl(SqsClient sqsClient, String queue_name) {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        return sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();
    }

    private void sendRegistrationMessage(SqsClient sqs, String app_name, String queue_name, String bucket_name, String file_name, String numberOfUrls) {
        System.out.println("Sending registration message");
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put(LOCAL_ID, MessageAttributeValue.builder().stringValue(app_name).dataType("String").build());
        messageAttributes.put(LOCAL_SQS_NAME, MessageAttributeValue.builder().stringValue(queue_name).dataType("String").build());
        messageAttributes.put(S_3_BUCKET_NAME, MessageAttributeValue.builder().stringValue(bucket_name).dataType("String").build());
        messageAttributes.put(S_3_BUCKET_KEY, MessageAttributeValue.builder().stringValue(file_name).dataType("String").build());
        messageAttributes.put(NUMBER_OF_URLS, MessageAttributeValue.builder().stringValue(numberOfUrls).dataType("String").build());
        messageAttributes.put("N", MessageAttributeValue.builder().stringValue(Integer.toString(this.N)).dataType("Number").build());

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(this.getQueueUrl(sqs, LOCALS_TO_MANAGER_SQS))
                .messageAttributes(messageAttributes)
                .messageBody("body from " + app_name)
                .build();
        sqs.sendMessage(sendMessageRequest);

    }

    private void createLocalQueue(SqsClient sqsClient, String local_name_queue) {
        System.out.println("Creating local queue: " + local_name_queue);
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(local_name_queue).build();
        sqsClient.createQueue(createQueueRequest);
    }

    private void createManagerIfNotRunning(Ec2Client ec2, SqsClient sqsClient) {
        System.out.println("checking if manager exist...");

        boolean managerIsRunning = false;
        String nextToken = null;

        try {

            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        for (Tag tag : instance.tags()) {
                            if (tag.value().equals(MANAGER)) {
                                //managerIsRunning = true;
                                if (!instance.state().name().toString().toLowerCase().equals("terminated") &&
                                        !instance.state().name().toString().toLowerCase().equals("stopped")) {
                                    // System.out.println("but is state is " + instance.state().name());
                                    managerIsRunning = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
            if (!managerIsRunning) {
                System.out.println("Creating manager...");
                createEc2Instance(MANAGER, AMI_ID, ec2);
                HashMap<QueueAttributeName, String> attributes = new HashMap<>();
                //attributes.put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, "20");
                CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                        .queueName(LOCALS_TO_MANAGER_SQS)
                        .attributes(attributes).build();
                sqsClient.createQueue(createQueueRequest);
                System.out.println("Manager is Running");
            } else {
                System.out.println("Manager is already running");
            }


        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


    private void createEc2Instance(String name, String ami_Id, Ec2Client ec2) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(ami_Id)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn(MANAGER_ARN).build())
                .instanceType(InstanceType.T2_MICRO)
                .userData(Base64.getEncoder().encodeToString(USER_DATA_MANAGER.getBytes()))
                .maxCount(1)
                .minCount(1)
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
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

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private String uploadFileToS3(S3Client s3, String bucket) {
        System.out.println("Uploading file to s3");
        createBucket(bucket, s3);
        String file_name = "file" + System.currentTimeMillis();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(file_name)
                .build();
        s3.putObject(putObjectRequest, Paths.get(this.getInputFileName() + ".txt"));

        System.out.println("Uploaded file: " + file_name + " to bucket: " + bucket);
        return file_name;
    }

    private void createBucket(String bucketName, S3Client s3) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .build());

        System.out.println(bucketName + " Created");
    }


    /**
     * checks if the argumnents given to the local application is valid.
     *
     * @param args array of strings to check
     * @return String which is the description of what is wrong. if nothing is wrong, return Null
     */
    public static String checkArguments(String[] args) {
        if (args.length == 0) {
            return "ERROR: No arguments were given";
        } else if (args[INPUT_FILE_NAME_INDEX].trim().equals("")) {
            return "ERROR: Input file name cannot be empty";
        } else if (args.length < 2) {
            return "ERROR: Output file name is missing";
        } else if (args[OUTPUT_FILE_NAME_INDEX].trim().equals("")) {
            return "ERROR: Output file name cannot be empty";
        } else if (args.length < 3) {
            return "ERROR: 'n' parameter is missing";
        } else if (!tryParseInt(args[N_INDEX])) {
            return "ERROR: 'n' parameter must be an integer number";
        } else {
            return null;
        }
    }

    /**
     * trying to parse a given string to int if possible
     *
     * @param toCheck string to check if it is an integer
     */
    public static boolean tryParseInt(String toCheck) {
        try {
            Integer.parseInt(toCheck);
            // parsing succeed
            return true;
        } catch (NumberFormatException e) {
            // parsing failed
            return false;
        }
    }
}