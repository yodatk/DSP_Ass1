package localApplication;


import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LocalApplication {

    public final static int INPUT_FILE_NAME_INDEX = 0;
    public final static int OUTPUT_FILE_NAME_INDEX = 1;
    public final static int N_INDEX = 2;
    public final static int TERMINATE_INDEX = 3;
    public final static Region REGION = Region.US_EAST_1;

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


    public void run() throws Exception {

        System.out.println("ALL GOOD...");
        String app_name = "LocalApp" + System.currentTimeMillis();
        String queue_name = app_name + "Queue";
        //todo - to check if we can put all locals on the same bucket
        final String bucket = "bucket" + System.currentTimeMillis();
        Ec2Client ec2 = Ec2Client.builder().region(REGION).build();
        SqsClient sqs_client = SqsClient.builder().region(REGION).build();
        S3Client s3 = S3Client.builder().region(REGION).build();
        createManagerIfNotRunning(ec2, sqs_client);
        String file_name = uploadFileToS3(s3, bucket);
        createLocalQueue(sqs_client, queue_name);
        // m = message from local application to manager
        sendRegistrationMessage(sqs_client, app_name, queue_name, bucket, file_name);

        boolean receive_ans = false;
        while (!receive_ans) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(this.getQueueUrl(sqs_client, queue_name))
                    .waitTimeSeconds(10)
                    .visibilityTimeout(1)
                    .build();
            List<Message> messages = sqs_client.receiveMessage(receiveMessageRequest).messages();
            for (Message m : messages) {
                if (m.body().startsWith("done")) {
                    receive_ans = true;
                    //todo- to match the names with the manger
                    String HTML_file = m.attributesAsStrings().get(HTML_FILE);
                    s3.getObject(GetObjectRequest.builder().bucket(bucket).key(HTML_file).build(), ResponseTransformer.toFile(Paths.get(this.getOutputFileName())));
                    System.out.println("HTML file is parsed and ready");
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

    public String getQueueUrl(SqsClient sqsClient, String queue_name) {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        return sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();
    }

    public void sendRegistrationMessage(SqsClient sqs, String app_name, String queue_name, String bucket_name, String file_name) {
        System.out.println("Sending registration message");
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put(LOCAL_ID, MessageAttributeValue.builder().dataType("String").stringValue(app_name).build());
        messageAttributes.put(LOCAL_SQS_NAME, MessageAttributeValue.builder().dataType("String").stringValue(queue_name).build());
        messageAttributes.put(S_3_BUCKET_NAME, MessageAttributeValue.builder().dataType("String").stringValue(bucket_name).build());
        messageAttributes.put(S_3_BUCKET_KEY, MessageAttributeValue.builder().dataType("String").stringValue(file_name).build());
        messageAttributes.put("N", MessageAttributeValue.builder().dataType("Number").stringValue(Integer.toString(this.N)).build());
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(this.getQueueUrl(sqs, LOCALS_TO_MANAGER_SQS))
                .messageAttributes(messageAttributes)
                .messageBody("body")
                .build();
        sqs.sendMessage(sendMessageRequest);

    }

    public void createLocalQueue(SqsClient sqsClient, String local_name_queue) {
        System.out.println("Creating local queue: " + local_name_queue);
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(local_name_queue).build();
        sqsClient.createQueue(createQueueRequest);
    }

    public void createManagerIfNotRunning(Ec2Client ec2, SqsClient sqsClient){
        System.out.println("checking if manager exist...");

        boolean manager_is_running = false;
        String nextToken = null;

        try {

            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        for (Tag tag : instance.tags())
                            if (tag.value().equals(MANAGER)) {
                                System.out.println("manager is already running");
                                manager_is_running = true;
                                break;
                            }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
            if (!manager_is_running) {
                System.out.println("Creating manager...");
                createEc2Instance(MANAGER, AMI_ID, ec2);
                HashMap<QueueAttributeName, String> attributes = new HashMap<>();
                //attributes.put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, "20");
                CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                        .queueName(LOCALS_TO_MANAGER_SQS)
                        .attributes(attributes).build();
                sqsClient.createQueue(createQueueRequest);
                System.out.println("Manager is Running");
            }

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }




    public void createEc2Instance(String name, String ami_Id, Ec2Client ec2) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(ami_Id)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn(MANAGER_ARN).build())
                .instanceType(InstanceType.T1_MICRO)
                .maxCount(1)
                .minCount(1)
                .build();
        //todo- toAdd the proper jar with .userData(Base64.getEncoder().encodeToString("script based on download the file from s3 and java -jar path_to_jar".getBytes()))
        //todo to add credentials in IAM role with .iamInstanceProfile(IamInstanceProfileSpecification.builder()
        //                        .arn(ARN).build())

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
                    "Successfully started EC2 Instance %s based on AMI %s",
                    instanceId, ami_Id);
            System.out.println("Successfully started EC2 Instance " + instanceId + " based on AMI " + ami_Id);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public String uploadFileToS3(S3Client s3, String bucket) {
        System.out.println("Uploading file to s3");
        createBucket(bucket, s3);
        String file_name = "file" + System.currentTimeMillis();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(file_name)
                .build();
        s3.putObject(putObjectRequest, Paths.get(this.getInputFileName()));

        System.out.println("Uploaded file: " + file_name + " to bucket: " + bucket);
        return file_name;
    }

    public void createBucket(String bucketName, S3Client s3) {
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