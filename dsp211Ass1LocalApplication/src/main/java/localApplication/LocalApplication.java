package localApplication;

//region imports

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
//endregion imports

/**
 * Task to run for the local application:
 * 1. Start the manager app with all needed parameters\ register itself to the services of the existing manager manager.
 * 2. Wait for notification from the manager that the job is done
 */
public class LocalApplication {

    /**
     * Index of the input file name in the main arguments array
     */
    public final static int INPUT_FILE_NAME_INDEX = 0;
    /**
     * Index of the output file name in the main arguments array
     */
    public final static int OUTPUT_FILE_NAME_INDEX = 1;
    /**
     * Index of the N - number of images per worker - in the main arguments array
     */
    public final static int N_INDEX = 2;
    /**
     * Index of the Terminate flag in the main arguments array
     */
    public final static int TERMINATE_INDEX = 3;
    /**
     * Default region where the aws services will be used
     */
    public final static Region REGION = Region.US_EAST_1;
    /**
     * String Constant for sqs messages from local application to manager application.
     * represents the name of the attributes of number of files to download from s3
     */
    public static final String NUM_OF_FILES = "numberOfFiles";
    /**
     * String constant for manager tag
     */
    public static final String MANAGER = "manager";
    /**
     * String constant for the name of the queue where local app put up tasks for manager to process
     */
    public static final String LOCALS_TO_MANAGER_SQS = "TASKS_FROM_LOCAL_QUEUE";
    /**
     * String constant to pass the ID of the current local Application in sqs messages
     */
    public static final String LOCAL_ID = "localID";
    /**
     * String Consant for name of the local sqs queue
     */
    public static final String LOCAL_SQS_NAME = "localSqsName";
    /**
     * String constant for sqs messages for bucket name attribute
     */
    public static final String S_3_BUCKET_NAME = "s3BucketName";
    /**
     * String constant for sqs messages for bucket key attribute
     */
    public static final String S_3_BUCKET_KEY = "s3BucketKey";
    /**
     * String constant for the AMI Id
     */
    public static final String AMI_ID = "ami-06ee5db2a0b25d160";
    /**
     * String constant for the Manager app ARN
     */
    public static final String MANAGER_ARN = "arn:aws:iam::192532717092:instance-profile/Manager-role";
    /**
     * String constant for sqs terminate message attribute
     */
    public static final String TERMINATE = "Terminate";
    /**
     * String constant for sqs attribute which is the number of urls to proccess
     */
    public static final String NUMBER_OF_URLS = "numberOfUrls";
    /**
     * String constant which is the prefix of the file to download from s3
     */
    public static final String TEMP_FILE_PREFIX = "tempfiles";

    /**
     * String constant which is the bash init command line the manager needs to run in aws to run properly
     */
    public static final String USER_DATA_MANAGER =
            "#!/bin/bash\n" +
                    "sudo mkdir /home/ass/\n" +
                    "sudo aws s3 cp s3://bucketforjar/Manager.jar /home/ass/\n" +
                    "sudo /usr/bin/java -jar /home/ass/Manager.jar\n" +
                    "shutdown -h now";

    /**
     * String :name of the input file argument
     */
    private String inputFileName;
    /**
     * String : name pf the outputfile argument
     */
    private String outputFileName;
    /**
     * integer: number of images to process per worker for this application
     */
    private int N;
    /**
     * boolean : flag to terminate whether after this local app finished , to terminate the manager's works
     */
    private boolean isTerminate;


    /**
     * constructor for the local application class
     *
     * @param args String array of arguments, including input file name, output file name, number of images per worker, and whether to terminate or not
     */
    public LocalApplication(String[] args) {
        this.inputFileName = args[INPUT_FILE_NAME_INDEX];
        this.outputFileName = args[OUTPUT_FILE_NAME_INDEX];
        this.N = Integer.parseInt(args[N_INDEX]);
        this.isTerminate = (args.length > 3 && args[TERMINATE_INDEX].equals("terminate"));
    }

    /**
     * inbput file name getter
     *
     * @return name of the input file
     */
    public String getInputFileName() {
        return inputFileName;
    }

    /**
     * count the number of urls in the input file
     *
     * @param filename name of the input file
     * @return integer: the number of urls in the file
     */
    private int countNumberOfUrls(String filename) {
        int counter = 0;
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String currentUrl;
            while ((currentUrl = br.readLine()) != null) {
                // as long as there is something to read in file
                if (currentUrl.trim().isEmpty()) {
                    // empty line -> don't count move ot next one
                    continue;
                } else {
                    // adding another url
                    counter++;
                }

            }
            return counter;
        } catch (IOException e) {
            //something went wrong -> returning -1 as an error flag
            e.printStackTrace();
            return -1;
        }

    }


    /**
     * main task of the local application :
     * 1. Start the manager app with all needed parameters\ register itself to the services of the existing manager manager.
     * 2. Wait for notification from the manager that the job is done
     */
    public void run() {

        System.out.println("Arguments check passed");
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

        sendRegistrationMessage(sqs_client, app_name, queue_name, bucket, file_name, numberOfUrls);

        // waiting for respond from manager
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
                    // manager approves the work is done -> starting converting the file to HTML
                    receive_ans = true;
                    Map<String, MessageAttributeValue> attributes = m.messageAttributes();

                    int numberOfFiles = Integer.parseInt(attributes.get(NUM_OF_FILES).stringValue());
                    // building first html tags
                    HtmlParser htmlParser = new HtmlParser(outputFileName);
                    boolean initResult = htmlParser.initFile();
                    if (!initResult) {
                        System.out.println("INIT HTML PARSING NOT WORKING!!!!!!!");
                    }
                    for (int i = 0; i < numberOfFiles; i++) {
                        // for each temp file in s3 -> download it, and add it to the html file
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
                    // adding end tags for html
                    boolean result = htmlParser.endFile();
                    if (result) {
                        System.out.println("HTML file is parsed and ready");
                    }
                    // deleting buckets and queues of this local application
                    emptyAndDeleteBucket(s3, bucket);
                    deleteQueue(sqs_client, queue_name);

                    break;
                }
            }
        }
        if (this.isTerminate) {
            // if this local got the terminate flag -> send it after html is done to the manager.
            System.out.println("Sending termination message");
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(this.getQueueUrl(sqs_client, LOCALS_TO_MANAGER_SQS))
                    .messageBody(TERMINATE)
                    .build();
            sqs_client.sendMessage(sendMessageRequest);
        }

    }


    /**
     * Delete empty bucket from s3
     *
     * @param s3     S3 client object ot send deletion message from
     * @param bucket name of the bucket to delete
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
     * Delete queue from sqs
     *
     * @param sqsClient SQS client object to send the delete message with
     * @param queueName name of the queue to delete
     */
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

    /**
     * get queue url from given queue name
     *
     * @param sqsClient SQS client object to send request
     * @param queueName name of the queue to get it's url
     * @return URL of the wanted queue
     */
    private String getQueueUrl(SqsClient sqsClient, String queueName) {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();
    }

    /**
     * registering this local application to the manager services, of parsing images urls to text
     *
     * @param sqs          SQS client object to send the message with
     * @param appName      String: name of this local app
     * @param queueName    String: name of the queue to send message when the task is finished
     * @param bucketName   String: gbucket name to send the urls with parsed text to
     * @param bucketKey    String: bucket key for this local application
     * @param numberOfUrls integer: number of the urls in the input file, and for the manger to parse
     */
    private void sendRegistrationMessage(SqsClient sqs, String appName, String queueName, String bucketName, String bucketKey, String numberOfUrls) {
        System.out.println("Sending registration message");
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put(LOCAL_ID, MessageAttributeValue.builder().stringValue(appName).dataType("String").build());
        messageAttributes.put(LOCAL_SQS_NAME, MessageAttributeValue.builder().stringValue(queueName).dataType("String").build());
        messageAttributes.put(S_3_BUCKET_NAME, MessageAttributeValue.builder().stringValue(bucketName).dataType("String").build());
        messageAttributes.put(S_3_BUCKET_KEY, MessageAttributeValue.builder().stringValue(bucketKey).dataType("String").build());
        messageAttributes.put(NUMBER_OF_URLS, MessageAttributeValue.builder().stringValue(numberOfUrls).dataType("String").build());
        messageAttributes.put("N", MessageAttributeValue.builder().stringValue(Integer.toString(this.N)).dataType("Number").build());

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(this.getQueueUrl(sqs, LOCALS_TO_MANAGER_SQS))
                .messageAttributes(messageAttributes)
                .messageBody("body from " + appName)
                .build();
        sqs.sendMessage(sendMessageRequest);

    }

    /**
     * creating Queue for this local app
     *
     * @param sqsClient      SQS client object to send the creation message with
     * @param localNameQueue String: name of the local queue to create
     */
    private void createLocalQueue(SqsClient sqsClient, String localNameQueue) {
        System.out.println("Creating local queue: " + localNameQueue);
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(localNameQueue).build();
        sqsClient.createQueue(createQueueRequest);
    }

    /**
     * creates a manager instance in AWS if it's not already running to parse the current task
     *
     * @param ec2       EC2 client object to preform the checking and the creation if necessary of the manager
     * @param sqsClient SQS client object to create the queue of registration to manager if necessary ot the manager
     */
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
            // something went wrong with the manager check \ creation
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


    /**
     * creating EC2 instance with the given name, and given AMI id
     *
     * @param name  String: name of the role tag of the ec2
     * @param amiId String : id of the image of the manager to run
     * @param ec2   EC2 client to send requests from
     */
    private void createEc2Instance(String name, String amiId, Ec2Client ec2) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
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
                    instanceId, amiId);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /**
     * Upload file to AWS S3 to the given bucket
     *
     * @param s3
     * @param bucket
     * @return name of the file that was uploaded to S3
     */
    private String uploadFileToS3(S3Client s3, String bucket) {
        System.out.println("Uploading file to s3");
        createBucket(bucket, s3);
        String fileName = "file" + System.currentTimeMillis();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(fileName)
                .build();
        s3.putObject(putObjectRequest, Paths.get(this.getInputFileName() + ".txt"));

        System.out.println("Uploaded file: " + fileName + " to bucket: " + bucket);
        return fileName;
    }

    /**
     * creates a bucket in AWS S3
     *
     * @param bucketName String: name of the bucket
     * @param s3         S3Client object to send request to open bucket with
     */
    private void createBucket(String bucketName, S3Client s3) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .build());

        System.out.println(bucketName + " Created");
    }


    /**
     * checks if the arguments given to the local application is valid.
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
     * try to parse the given string as int if possible
     *
     * @param toCheck String to check as integer
     * @return Boolean: if String is integer,return true, otherwise false
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