package worker;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class WorkerMain {

    private static S3Client s3;

    public static void main(String[] args) {
        System.out.println("check check check check");

    }

    private static void oldMain(String[] args) {
        if (args.length < 1) {
            System.out.println("AWS key must be inserted");
            return;
        }

        String awsKey = args[0];
        while (true) {
            String url = getNextmessage();

            if (url.equals("terminate")) {
                return;
            }

            Worker w = new Worker();
            String output = w.newImageTaskWithTessaract(url);

            putProccessedDataInBacket(url, output);
        }


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
//        }
    }

    private static void checkImageParsing() {
        Worker w = new Worker();
        String[] arr = new String[]{
        "http://ct.mob0.com/Fonts/CharacterMap/ocraextended.png",
        "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
        "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
        "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
        "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
        "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
        "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
        "http://www.identifont.com/samples/bitstream/OCRA.gif",
        "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
        "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
        "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.pnghttp://ct.mob0.com/Fonts/CharacterMap/ocraextended.png",
        "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
        "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
        "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
        "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
        "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
        "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
        "http://www.identifont.com/samples/bitstream/OCRA.gif",
        "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
        "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
        "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
        "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
        "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
        "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
        "http://www.identifont.com/samples/bitstream/OCRA.gif",
        "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
        "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
        "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
        "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
        "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
        "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
        "http://www.identifont.com/samples/bitstream/OCRA.gif",
        "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
        "http://www.barcodesoft.com/barcode-image/ocramapping.jpg",
        "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
        "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
        "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
        "http://www.identifont.com/samples/bitstream/OCRA.gif",
        "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
        "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
        "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
        "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
        "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
        "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
        "http://www.identifont.com/samples/bitstream/OCRA.gif",
        "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
        "http://www.barcodesoft.com/barcode-image/ocramapping.jpg"
};

        Map<String,String> urlsWithParsing = new HashMap<>();
        for (String url : arr) {
            System.out.println();
            String parsed = w.newImageTaskWithTessaract(url);
            urlsWithParsing.put(url,parsed);
        }
        HtmlParserTemp htmlParserTemp = new HtmlParserTemp();
        htmlParserTemp.parseListOfUrlAndTextToHTML(urlsWithParsing,"long_file");
    }

    public static void putProccessedDataInBacket(String url, String output) {
        //todo
    }

    public static String getNextmessage() {
        //todo get link from s3
        String url = "not real url";
        return url;
    }

    private static void createBucket(String bucket, Region region) {
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

    /**
     * Uploading an object to S3 in parts
     */
    private static void multipartUpload(String bucketName, String key) throws IOException {

        int mb = 1024 * 1024;
        // First create a multipart upload and get upload id
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName).key(key)
                .build();
        CreateMultipartUploadResponse response = s3.createMultipartUpload(createMultipartUploadRequest);
        String uploadId = response.uploadId();
        System.out.println(uploadId);

        // Upload all the different parts of the object
        UploadPartRequest uploadPartRequest1 = UploadPartRequest.builder().bucket(bucketName).key(key)
                .uploadId(uploadId)
                .partNumber(1).build();
        String etag1 = s3.uploadPart(uploadPartRequest1, RequestBody.fromByteBuffer(getRandomByteBuffer(5 * mb))).eTag();
        CompletedPart part1 = CompletedPart.builder().partNumber(1).eTag(etag1).build();

        UploadPartRequest uploadPartRequest2 = UploadPartRequest.builder().bucket(bucketName).key(key)
                .uploadId(uploadId)
                .partNumber(2).build();
        String etag2 = s3.uploadPart(uploadPartRequest2, RequestBody.fromByteBuffer(getRandomByteBuffer(3 * mb))).eTag();
        CompletedPart part2 = CompletedPart.builder().partNumber(2).eTag(etag2).build();


        // Finally call completeMultipartUpload operation to tell S3 to merge all uploaded
        // parts and finish the multipart operation.
        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(part1, part2).build();
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                CompleteMultipartUploadRequest.builder().bucket(bucketName).key(key).uploadId(uploadId)
                        .multipartUpload(completedMultipartUpload).build();
        s3.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private static ByteBuffer getRandomByteBuffer(int size) throws IOException {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return ByteBuffer.wrap(b);
    }
}
