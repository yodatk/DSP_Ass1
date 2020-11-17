package localApplication;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.HashMap;


public class LocalApplication {

    public final static int INPUT_FILE_NAME_INDEX = 0;
    public final static int OUTPUT_FILE_NAME_INDEX = 1;
    public final static int N_INDEX = 2;
    public final static int TERMINATE_INDEX = 3;
    public final static Region REGION = Region.US_WEST_2;
    public static final  String MANAGER = "manager";
    public static final String REGISTRATION_QUEUE = "RegistrationQueue";

    private String inputFileName;
    private String outputFileName;
    private int N;
    private boolean isTerminate;

    public LocalApplication(String inputFileName, String outputFileName, int n, boolean isTerminate) {
        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
        N = n;
        this.isTerminate = isTerminate;
    }

    public LocalApplication(String[] args) {
        this(
                args[INPUT_FILE_NAME_INDEX],
                args[OUTPUT_FILE_NAME_INDEX],
                Integer.parseInt(args[N_INDEX]),
                (args.length > 3 && args[TERMINATE_INDEX].equals("terminate"))
        );
    }

    public String getInputFileName() {
        return inputFileName;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public int getN() {
        return N;
    }

    public boolean isTerminate() {
        return isTerminate;
    }

    public void CreateManagerIfNotRunning(Ec2Client ec2, SqsClient sqsClient) throws Exception{
        System.out.println("check if manager exist");

        boolean manager_is_running = false;
        String nextToken = null;

        try {

            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        for (Tag tag : instance.tags())
                            if(tag.value().equals(MANAGER)){
                                System.out.println("manager is already running");
                                manager_is_running = true;
                                break;
                                
                            }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
            if (!manager_is_running){
                System.out.println("Creating manager...");
                HashMap<QueueAttributeName,String> attributes = new HashMap<>();
                //attributes.put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, "20");
                CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                        .queueName(REGISTRATION_QUEUE)
                        .attributes(attributes).build();
                sqsClient.createQueue(createQueueRequest);
                //TODO create instance of manager with proper AMI
            }

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public void run() throws Exception {
        if (this.isTerminate) {
            System.out.println("terminate");
        }
        System.out.println("ALL GOOD...");
        String app_name = "LocalApp" + System.currentTimeMillis();
        Ec2Client ec2 = Ec2Client.builder().region(REGION).build();
        SqsClient sqsClient = SqsClient.builder().region(REGION).build();
        CreateManagerIfNotRunning(ec2,sqsClient);

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