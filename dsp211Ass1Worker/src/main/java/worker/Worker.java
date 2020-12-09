package worker;
//region imports

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sqs.model.UnsupportedOperationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
//endregion imports

/**
 * Class in charge of the work flow of the worker flow:
 * 1. Take an url to parse from the tasks to do queue
 * 2. parse it with the OCRParser class
 * 3. send result to manager
 * and repeat stage 1-3 until termination
 */
public class Worker implements Runnable {

    /**
     * String constant of the name of the queue to take tasks from
     */
    public static final String WORKERS_SQS = "TO_DO_QUEUE"; // sqs for workers
    /**
     * String constant of the name of the queue to send completed task to
     */
    public static final String WORKERS_TO_MANAGER_SQS = "COMPLETED_IMAGES_QUEUE"; // sqs for MANAGER to get messages from workers
    /**
     * String constant of the region of AWS services
     */
    public static final Region REGION = Region.US_EAST_1;
    /**
     * integer represents the time in seconds before the message the worker took from the queue become visible to hte other workers again
     */
    public static final int IMAGE_PARSING_TIME_OUT_IN_SEC = 200;
    /**
     * String : attribute name of the parsed text in sqs message
     */
    public static final String PARSED_TEXT = "parsedText";
    /**
     * String: attribute name of the id of the local application the url belong to
     */
    public static final String LOCAL_ID = "localID";
    /**
     * String: attribute name of the Image url to send in sqs message
     */
    public static final String IMAGE_URL = "imageUrl";
    /**
     * String: url of the tasks to do queue in SQS
     */
    private String queueWorkersUrl;
    /**
     * String: url of the completed tasks queue
     */
    private String queueWorkersToManagersUrl;
    /**
     * sqs Client to get and send messages.
     */
    private SqsClient sqs;
    /**
     * OCR object to parse images url to text from image
     */
    private OCRParser ocrWorker;

    /**
     * Constructor for the Worker Object
     */
    public Worker() {
        this.ocrWorker = new OCRParser();
    }

    /**
     * initializing all aws object and urls to establish communication
     */
    private void init() {
        this.sqs = SqsClient.builder().region(REGION).build();
        GetQueueUrlRequest getWorkersQueueRequest = GetQueueUrlRequest.builder()
                .queueName(WORKERS_SQS)
                .build();
        this.queueWorkersUrl = sqs.getQueueUrl(getWorkersQueueRequest).queueUrl();
        GetQueueUrlRequest getWorkersToManagerQueueRequest = GetQueueUrlRequest.builder()
                .queueName(WORKERS_TO_MANAGER_SQS)
                .build();
        this.queueWorkersToManagersUrl = sqs.getQueueUrl(getWorkersToManagerQueueRequest).queueUrl();

    }

    /**
     * main worker task loop :
     * 1. Take an url to parse from the tasks to do queue
     * 2. parse it with the OCRParser class
     * 3. send result to manager
     * and repeat stage 1-3 until termination
     */
    public void run() {
        //initializing
        init();
        //repeat until termination
        while (true) {
            // checking for tasks from messages queues
            ReceiveMessageRequest receiveMessagesFromManager = ReceiveMessageRequest.builder()
                    .queueUrl(queueWorkersUrl)
                    .visibilityTimeout(IMAGE_PARSING_TIME_OUT_IN_SEC)
                    .maxNumberOfMessages(1)
                    .messageAttributeNames("All")
                    .build();
            List<Message> tasksFromManager = this.sqs.receiveMessage(receiveMessagesFromManager).messages();
            for (Message msg : tasksFromManager) {
                // suppose to only one message if at all

                Map<String, MessageAttributeValue> givenAttributes = msg.messageAttributes();
                String localId = givenAttributes.get(LOCAL_ID).stringValue();
                String imagerUrl = givenAttributes.get(IMAGE_URL).stringValue();
                // parsing image
                String parsedText = this.ocrWorker.newImageTaskWithTessaract(imagerUrl, localId);

                // after image is proccessed-> sending result to manager
                Map<String, MessageAttributeValue> attr = new HashMap<>();
                attr.put(LOCAL_ID, MessageAttributeValue.builder().dataType("String").stringValue(localId).build());
                attr.put(IMAGE_URL, MessageAttributeValue.builder().dataType("String").stringValue(imagerUrl).build());
                attr.put(PARSED_TEXT, MessageAttributeValue.builder().dataType("String").stringValue(parsedText).build());
                boolean success = false;
                long startTime = System.currentTimeMillis();
                // if didn't succeed yet, and time out hasn't come yet
                while (!success && System.currentTimeMillis() - startTime < 100000) {
                    try {
                        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                                .queueUrl(queueWorkersToManagersUrl)
                                .messageBody(localId + " " + imagerUrl)
                                .messageAttributes(attr)
                                .delaySeconds(5)
                                .build();
                        SendMessageResponse response = sqs.sendMessage(sendMessageRequest);
                        System.out.println("url " + imagerUrl + " is done and sent message to sqs " + WORKERS_TO_MANAGER_SQS);
                        if (response.sdkHttpResponse().isSuccessful()) {
                            success = true;
                            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                                    .queueUrl(queueWorkersUrl)
                                    .receiptHandle(msg.receiptHandle())
                                    .build();
                            sqs.deleteMessage(deleteMessageRequest);
                        }
                    } catch (InvalidMessageContentsException | UnsupportedOperationException e) {
                        System.out.println("Error found while sending the message: ");
                        e.printStackTrace();
                        success = false;
                    }
                }
            }

        }
    }
}

