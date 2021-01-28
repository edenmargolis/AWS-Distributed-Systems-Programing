package com.example.project;
import net.sourceforge.tess4j.Tesseract;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import javax.imageio.ImageIO;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;

public class Worker {

    static String managerUrl;
    static String workerUrl;

    public static void main (String [] args) throws Exception {

        String Delimiter = "HereComesDelimiter";

        //create SQS and S3 client

        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        S3Client s3 = S3Client.builder().build();

        //gets worker and manager queues.
        List<Bucket> bucketList = s3.listBuckets().buckets();
        String bucket = bucketList.get(0).name();
        managerUrl = getObject(s3, bucket,"manager");
        workerUrl = getObject(s3, bucket,"worker");

        // waiting for massages

        int size = 0;
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(workerUrl)
                .maxNumberOfMessages(1)
                .build();
        Message message;
        boolean terminate = false;
        while (!terminate) {

            List<Message> messages = null;
            while (size==0){
                messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
                size = messages.size();
            }
            size--;
            message = messages.get(0);

            String body = message.body();
            String[] parts = body.split(Delimiter,2);
            if(parts [0].equals("new image task")) {
                String[] parts1 = parts[1].split(Delimiter,2);
                Tesseract tesseract = new Tesseract();
                tesseract.setDatapath("/home/ubuntu");
                String text;
                try {
                    text = tesseract.doOCR(ImageIO.read(new URL(parts1[0]).openStream()));
                }
                catch (Exception e){ //send as answer the error we got
                    text="picDoesNotExist"+e.toString();
                }

                //send massage to manager
                sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(managerUrl).messageBody("done OCR task" + Delimiter + parts1[0] + Delimiter + text + Delimiter + parts1[1]).build());

                //delete the message from the queue
                deleteMessage(sqsClient, workerUrl, message);

            }
            else if (parts [0].equals("terminate")){
                deleteMessage(sqsClient, workerUrl, message);
                Ec2Client ec2 = Ec2Client.create();
                String instanceId = EC2MetadataUtils.getInstanceId();
                ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instanceId).build());
                terminate = true;
            }
        }
    }

    private static String getObject (S3Client s3, String bucket, String key) throws IOException {

        ResponseInputStream<GetObjectResponse> object = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
        BufferedReader reader = new BufferedReader(new InputStreamReader(object));
        String line;
        String finish ="";
        while ((line = reader.readLine()) != null) {
            finish = finish+line;
        }
        return finish;
    }


    private static void deleteMessage(SqsClient sqsClient, String queueUrl,  Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }
}