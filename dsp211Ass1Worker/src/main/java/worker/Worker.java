package worker;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sqs.model.UnsupportedOperationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Worker {

    public static final String WOKERS_SQS = "TO_DO_QUEUE"; // sqs for workers
    public static final String WORKERS_TO_MANAGER_SQS = "COMPLETED_IMAGES_QUEUE"; // sqs for MANAGER to get messages from workers
    public static final Region REGION = Region.US_EAST_1;
    public static final int IMAGE_PARSING_TIME_OUT_IN_SEC = 300;


    public static final String PARSED_TEXT = "parsedText";
    public static final String LOCAL_ID = "localID";
    public static final String IMAGE_URL = "imageUrl";

    private String queueWorkersUrl;
    private String queueWorkersToManagersUrl;
    private SqsClient sqs;
    private OCRParser ocrWorker;

    public Worker() {
        this.ocrWorker = new OCRParser();
    }

    private void init() {
        this.sqs = SqsClient.builder().region(REGION).build();
        GetQueueUrlRequest getWorkersQueueRequest = GetQueueUrlRequest.builder()
                .queueName(WOKERS_SQS)
                .build();
        this.queueWorkersUrl = sqs.getQueueUrl(getWorkersQueueRequest).queueUrl();
        GetQueueUrlRequest getWorkersToManagerQueueRequest = GetQueueUrlRequest.builder()
                .queueName(WORKERS_TO_MANAGER_SQS)
                .build();
        this.queueWorkersToManagersUrl = sqs.getQueueUrl(getWorkersToManagerQueueRequest).queueUrl();

    }

    public void run() {
        init();
        while (true) {
            // checking for tasks from messages queues
            ReceiveMessageRequest receiveMessagesFromManager = ReceiveMessageRequest.builder()
                    .queueUrl(queueWorkersUrl)
                    .visibilityTimeout(IMAGE_PARSING_TIME_OUT_IN_SEC)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> tasksFromManager = this.sqs.receiveMessage(receiveMessagesFromManager).messages();
            for (Message msg : tasksFromManager) {
                // suppose to only one message if at all

                Map<String, String> givenAttributes = msg.attributesAsStrings();
                String localId = givenAttributes.get(LOCAL_ID);
                String imagerUrl = givenAttributes.get(IMAGE_URL);
                // parsing image
                String parsedText = this.ocrWorker.newImageTaskWithTessaract(imagerUrl,localId);

                // after image is proccessed-> sending result to manager
                Map<String, MessageAttributeValue> attr = new HashMap<>();
                attr.put(LOCAL_ID, MessageAttributeValue.builder().stringValue(localId).build());
                attr.put(IMAGE_URL, MessageAttributeValue.builder().stringValue(imagerUrl).build());
                attr.put(PARSED_TEXT, MessageAttributeValue.builder().stringValue(parsedText).build());
                boolean success = false;
                long startTime = System.currentTimeMillis();
                // if didn't succeed yet, and time out hasn't come yet
                while (!success && System.currentTimeMillis() - startTime > 100000) {
                    try {
                        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                                .queueUrl(queueWorkersToManagersUrl)
                                .messageBody(localId + " " + imagerUrl)
                                .messageAttributes(attr)
                                .delaySeconds(5)
                                .build();
                        SendMessageResponse response = sqs.sendMessage(sendMessageRequest);
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




// region old

//    private static void oldMain(String[] args) {
//        if (args.length < 1) {
//            System.out.println("AWS key must be inserted");
//            return;
//        }
//
//        String awsKey = args[0];
//        while (true) {
//            String url = getNextmessage();
//
//            if (url.equals("terminate")) {
//                return;
//            }
//
//            OCRParser w = new OCRParser();
//            String output = w.newImageTaskWithTessaract(url);
//
//            putProccessedDataInBacket(url, output);
//        }
//
//
//    }

//
//    public static void putProccessedDataInBacket(String url, String output) {
//        //todo
//    }
//
//    public static String getNextmessage() {
//        //todo get link from s3
//        String url = "not real url";
//        return url;
//    }
//
//    private static void createBucket(String bucket, Region region) {
//        s3.createBucket(CreateBucketRequest
//                .builder()
//                .bucket(bucket)
//                .createBucketConfiguration(
//                        CreateBucketConfiguration.builder()
//                                .locationConstraint(region.id())
//                                .build())
//                .build());
//
//        System.out.println(bucket);
//    }
//
//    /**
//     * Uploading an object to S3 in parts
//     */
//    private static void multipartUpload(String bucketName, String key) throws IOException {
//
//        int mb = 1024 * 1024;
//        // First create a multipart upload and get upload id
//        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
//                .bucket(bucketName).key(key)
//                .build();
//        CreateMultipartUploadResponse response = s3.createMultipartUpload(createMultipartUploadRequest);
//        String uploadId = response.uploadId();
//        System.out.println(uploadId);
//
//        // Upload all the different parts of the object
//        UploadPartRequest uploadPartRequest1 = UploadPartRequest.builder().bucket(bucketName).key(key)
//                .uploadId(uploadId)
//                .partNumber(1).build();
//        String etag1 = s3.uploadPart(uploadPartRequest1, RequestBody.fromByteBuffer(getRandomByteBuffer(5 * mb))).eTag();
//        CompletedPart part1 = CompletedPart.builder().partNumber(1).eTag(etag1).build();
//
//        UploadPartRequest uploadPartRequest2 = UploadPartRequest.builder().bucket(bucketName).key(key)
//                .uploadId(uploadId)
//                .partNumber(2).build();
//        String etag2 = s3.uploadPart(uploadPartRequest2, RequestBody.fromByteBuffer(getRandomByteBuffer(3 * mb))).eTag();
//        CompletedPart part2 = CompletedPart.builder().partNumber(2).eTag(etag2).build();
//
//
//        // Finally call completeMultipartUpload operation to tell S3 to merge all uploaded
//        // parts and finish the multipart operation.
//        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(part1, part2).build();
//        CompleteMultipartUploadRequest completeMultipartUploadRequest =
//                CompleteMultipartUploadRequest.builder().bucket(bucketName).key(key).uploadId(uploadId)
//                        .multipartUpload(completedMultipartUpload).build();
//        s3.completeMultipartUpload(completeMultipartUploadRequest);
//    }
//
//    private static ByteBuffer getRandomByteBuffer(int size) throws IOException {
//        byte[] b = new byte[size];
//        new Random().nextBytes(b);
//        return ByteBuffer.wrap(b);
//    }
//
//
//    private static void checkImageParsing() {
//        OCRParser w = new OCRParser();
//        String[] arr = new String[]{
//                "http://ct.mob0.com/Fonts/CharacterMap/ocraextended.png",
//                "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
//                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
//                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
//                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
//                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
//                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
//                "http://www.identifont.com/samples/bitstream/OCRA.gif",
//                "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
//                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
//                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.pnghttp://ct.mob0.com/Fonts/CharacterMap/ocraextended.png",
//                "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
//                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
//                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
//                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
//                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
//                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
//                "http://www.identifont.com/samples/bitstream/OCRA.gif",
//                "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
//                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
//                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
//                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
//                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
//                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
//                "http://www.identifont.com/samples/bitstream/OCRA.gif",
//                "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
//                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
//                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
//                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
//                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
//                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
//                "http://www.identifont.com/samples/bitstream/OCRA.gif",
//                "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
//                "http://www.barcodesoft.com/barcode-image/ocramapping.jpg",
//                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
//                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
//                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
//                "http://www.identifont.com/samples/bitstream/OCRA.gif",
//                "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
//                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
//                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
//                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
//                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
//                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
//                "http://www.identifont.com/samples/bitstream/OCRA.gif",
//                "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
//                "http://www.barcodesoft.com/barcode-image/ocramapping.jpg"
//        };
//
//        Map<String, String> urlsWithParsing = new HashMap<>();
//        for (String url : arr) {
//            System.out.println();
//            String parsed = w.newImageTaskWithTessaract(url);
//            urlsWithParsing.put(url, parsed);
//        }
//        HtmlParserTemp htmlParserTemp = new HtmlParserTemp();
//        htmlParserTemp.parseListOfUrlAndTextToHTML(urlsWithParsing, "long_file");
//    }
//}


//        Region region = Region.US_WEST_2;
//        s3 = S3Client.builder().region(region).build();
//
//
//        String bucket = "bucket" + System.currentTimeMillis();
//        String bucketKey = "b_key";
//
//        createBucket(bucket, region);
//
//        // Put Object
//        try {
//            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(bucketKey)
//                            .build(),
//                    RequestBody.fromByteBuffer(getRandomByteBuffer(10_000)));
//
//            // Multipart Upload a file
//            String multipartKey = "multiPartKey";
//            multipartUpload(bucket, multipartKey);
//
//        } catch (IOException e) {
//
    // endregion old
}

