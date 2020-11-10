package manager;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ManagerMain {
    public static final String WOKERS_SQS = "TO_DO_QUEUE"; // sqs for workers
    public static final String MANAGER_SQS = "COMPLETED_QUEUE"; // sqs for MANAGER
    public static final String IMAGE_URL = "imageUrl";
    public static final String INDEX = "index";

    public static void main(String[] args) {


        Manager manager = new Manager();


        if (args.length < 3) {
            System.out.println("AWS key, images location, and 'n' must be inserted");
            return;
        }


        String awsKey = args[0];
        String imagesLocationInS3 = args[1];
        int n = Integer.parseInt(args[2]);
        int numberOfImages = getNumberOfImages();
        // creating workers
        Map<String, Ec2Client> workersMap = createWorkers(n);

        SqsClient sqs = SqsClient.builder().region(Region.US_WEST_2).build();
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(WOKERS_SQS)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            e.printStackTrace();

        }

        GetQueueUrlRequest getWorkersQueueRequest = GetQueueUrlRequest.builder()
                .queueName(WOKERS_SQS)
                .build();
        String queueWorkersUrl = sqs.getQueueUrl(getWorkersQueueRequest).queueUrl();

        GetQueueUrlRequest getManagerQueueRequest = GetQueueUrlRequest.builder()
                .queueName(MANAGER_SQS)
                .build();
        String queueManagersUrl = sqs.getQueueUrl(getManagerQueueRequest).queueUrl();

        // give work to workers
        for (int i = 0; i < numberOfImages; i++) {
            String imageUrl = getCurrentImage(i);

            Map<String, MessageAttributeValue> attr = new HashMap<>();
            attr.put(INDEX, MessageAttributeValue.builder().stringValue(Integer.toString(i)).build());
            attr.put(IMAGE_URL, MessageAttributeValue.builder().stringValue(imageUrl).build());
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                    .queueUrl(queueWorkersUrl)
                    .messageBody(i + " " + imageUrl)
                    .messageAttributes(attr)
                    .delaySeconds(5)
                    .build();
            sqs.sendMessage(send_msg_request);
        }

        while (true) {
            // receive messages from the queue
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueManagersUrl)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            for (Message m : messages) {
                if(m.body().equals("terminate")){
                    break;
                }
                Map<String, MessageAttributeValue> messageParams = m.messageAttributes();
                String imageIndex = messageParams.get(INDEX).stringValue();
                // delete completed image from s3
                deleteImageFromS3(imageIndex);
            }

        }
//        String imageUrl = messageParams.get(IMAGE_URL).stringValue();
//        String parsed = getParsedDataFromS3(imageUrl);
    }

    public static String getParsedDataFromS3(String imageUrl){

        //todo
        return "";
    }

    public static void deleteImageFromS3(String ImageIndex){
        //todo
    }

    public static String getCurrentImage(int index) {
        //todo
        return "todo";
    }

    public static int getNumberOfImages() {
        //todo
        return 1;
    }

    public static Map<String, Ec2Client> createWorkers(int n) {
        // todo
        int numberOfWorkers = getNumberOfImages() / n;
        return new HashMap<String, Ec2Client>();
    }
}
