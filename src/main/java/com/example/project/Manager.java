package com.example.project;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Base64;
import java.util.List;

public class Manager {

    static String managerUrl;
    static String workerUrl;
    static boolean terminate = false;

    public static void main (String [] args) throws Exception {
        
        String Delimiter = "HereComesDelimiter";

        //create SQS and S3 client
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        S3Client s3 = S3Client.builder().build();

        //get the URL of the manager's queue

        List<Bucket> bucketList = s3.listBuckets().buckets();
        String managerBucket = bucketList.get(0).name();

        managerUrl = getObject(s3, managerBucket,"manager");

        //create a queue for all the workers

        workerUrl = createQueue(sqsClient);
        putInputObject(s3, managerBucket, "worker:"+ workerUrl);

        int amountOfLocalApps = 0; //number of all the active locals

        while (!terminate) {

            // waiting for massages
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(managerUrl)
                    .maxNumberOfMessages(1)
                    .visibilityTimeout(20)
                    .build();
            Message message;
            List<Message> messages = null;
            int size = 0;
            while (size==0){
                messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
                size = messages.size();
            }
            size--;
            message = messages.get(0);
            String body = message.body();

            String[] parts = body.split(Delimiter, 2);

            deleteMessage(sqsClient, managerUrl, message);

            switch (parts[0]) {
                case "new task": {

                    amountOfLocalApps++;
                    String[] parts1 = parts[1].split(Delimiter, 2);
                    String key = parts1[0];
                    String localApp = parts1[1];

                    putInputObject(s3, localApp, "summaryFile:<html>\n<body>\n\n");

                    InputStream imageFile;
                    imageFile = s3.getObject(GetObjectRequest.builder().bucket(localApp).key(key).build());

                    BufferedReader reader = new BufferedReader(new InputStreamReader(imageFile));

                    //sending each url image as a sqs message to worker sqs.
                    int jobsCount = 0; //the number of URLs in the file.
                    String line;
                    int n = Integer.parseInt(getObject(s3, localApp, "n"));
                    int workers = 0;

                    try {
                        while (!(line = reader.readLine()).equals("")) {
                            if (jobsCount % n == 0) {
                                workers++;
                            }
                            jobsCount++;
                            sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(workerUrl).messageBody("new image task" + Delimiter + line + Delimiter + localApp).build());
                        }
                    } catch (Exception e) {}

                    imageFile.close();

                    putInputObject(s3, localApp, "number:" + jobsCount);
                    putInputObject(s3, localApp, "workersAmount:" + workers);
                    for (int i = 0; i < workers; i++) //create some workers according to n value
                       CreateWorkers();
                    break;
                }
                case "done OCR task": {
                    String[] parts1 = parts[1].split(Delimiter, 3);

                    String imageLink = parts1[0];
                    String OCRoutput = parts1[1];
                    String localBucket = parts1[2];
                    String summaryFile = getObject(s3, localBucket, "summaryFile");

                    if (OCRoutput.contains("picDoesNotExist")) { //if Tessercat return error, we are not printing the image. we are printing instead the error
                        summaryFile += "<p><b>The image with URL " + imageLink + " has some problems. Here's the exception:</b></p>\n";
                        summaryFile += "<p>" + OCRoutput.split("picDoesNotExist", 2)[1] + "</p>\n";
                    } else {
                        summaryFile += "<img src= \"" + imageLink + "\" alt=\"Trulli\" width=\"500\" height=\"333\">\n";
                        summaryFile += "<p>" + OCRoutput + "</p>\n";
                    }
                    int counter = Integer.parseInt(getObject(s3, localBucket, "number"));
                    counter--;
                    if (counter == 0) { //checking if we have to wait to another answers.
                        summaryFile += "\n</body>\n </html>\n";

                        s3.putObject(PutObjectRequest.builder().bucket(localBucket).key("SummaryFile")
                                .build(), RequestBody.fromString(summaryFile));

                        try {
                            sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(getObject(s3, localBucket, "local")).messageBody("done task").build());
                        } catch (Exception e) {}

                        int workers = Integer.parseInt(getObject(s3, localBucket, "workersAmount"));
                        for (int i = 0; i < workers; i++)
                            sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(workerUrl).messageBody("terminate" + Delimiter).build());
                        amountOfLocalApps--;
                    } else {
                        putInputObject(s3, localBucket, "number:" + counter); //update number of URLs
                        putInputObject(s3, localBucket, "summaryFile:" + summaryFile); //update summary file.
                    }
                    break;
                }
                case "terminate":

                    while (amountOfLocalApps > 0) { //check if there are more active locals to wait for.
                        receiveMessageRequest = ReceiveMessageRequest.builder()
                                .queueUrl(managerUrl)
                                .maxNumberOfMessages(1)
                                .visibilityTimeout(20)
                                .build();

                        while (size <= 0) {
                            messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
                            size = messages.size();
                        }
                        message = sqsClient.receiveMessage(receiveMessageRequest).messages().get(0);

                        body = message.body();
                        parts = body.split(Delimiter, 2);
                        deleteMessage(sqsClient, managerUrl, message);

                        if (parts[0].equals("done OCR task")) {
                            String[] parts1 = parts[1].split(Delimiter, 3);

                            String imageLink = parts1[0];
                            String OCRoutput = parts1[1];
                            String localBucket = parts1[2];
                            String summaryFile = getObject(s3, localBucket, "summaryFile");

                            if (OCRoutput.contains("picDoesNotExist")) {
                                summaryFile += "<p><b>The image with URL " + imageLink + " has some problems. Here's the exception:</b></p>\n";
                                summaryFile += "<p>" + OCRoutput.split("picDoesNotExist", 2)[1] + "</p>\n";
                            } else {
                                summaryFile += "<img src=\"" + imageLink + "\" alt=\"Trulli\" width=\"500\" height=\"333\">\n";
                                summaryFile += "<p>" + OCRoutput + "</p>\n";
                            }

                            int counter = Integer.parseInt(getObject(s3, localBucket, "number"));
                            counter--;
                            if (counter == 0) {
                                summaryFile += "\n</body>\n </html>\n";

                                s3.putObject(PutObjectRequest.builder().bucket(localBucket).key("SummaryFile")
                                        .build(), RequestBody.fromString(summaryFile));
                                try {
                                    sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(getObject(s3, localBucket, "local")).messageBody("done task").build());
                                } catch (Exception e) {}
                                int workers = Integer.parseInt(getObject(s3, localBucket, "workersAmount"));
                                for (int i = 0; i < workers; i++)
                                    sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(workerUrl).messageBody("terminate" + Delimiter).build());
                                amountOfLocalApps--;
                            } else {
                                putInputObject(s3, localBucket, "number:" + counter);
                                putInputObject(s3, localBucket, "summaryFile:" + summaryFile);
                            }
                        }
                    }

                    //delete Worker instances

                    Ec2Client ec2 = Ec2Client.create();
                    List<Reservation> reservations = ec2.describeInstances().reservations();
                    for (Reservation reservation : reservations) {
                        for (Instance instance : reservation.instances()) {
                            List<Tag> tags = instance.tags();
                            for (Tag tag : tags)
                                if (tag.value().equals("worker"))
                                    ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build());
                        }
                    }

                    //delete all SQS

                    ListQueuesResponse queues = sqsClient.listQueues();
                    for (String url : queues.queueUrls()) {
                        DeleteQueueRequest req = DeleteQueueRequest.builder().queueUrl(url).build();
                        sqsClient.deleteQueue(req);
                    }

                    //Delete the remaining bucket

                    ListObjectsRequest request = ListObjectsRequest.builder().bucket(bucketList.get(0).name()).build();
                    ListObjectsResponse objectList = s3.listObjects(request);
                    for (S3Object s3Object : objectList.contents()) {
                        String key = s3Object.key();
                        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketList.get(0).name()).key(key).build();
                        s3.deleteObject(deleteObjectRequest);
                    }
                    DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketList.get(0).name()).build();
                    s3.deleteBucket(deleteBucketRequest);

                    //delete ec2 instances of manager

                    ec2 = Ec2Client.create();
                    String instanceId = EC2MetadataUtils.getInstanceId();
                    ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instanceId).build());
                    break;
            }
        }
    }

    private static String getObject (S3Client s3, String bucket, String key) throws IOException {

        ResponseInputStream<GetObjectResponse> object = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
        BufferedReader reader = new BufferedReader(new InputStreamReader(object));
        String line;
        StringBuilder finish = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            finish.append(line);
        }
        return finish.toString();
    }

    private static void CreateWorkers (){

            Ec2Client ec2 = Ec2Client.create();

            IamInstanceProfileSpecification role = IamInstanceProfileSpecification.builder().arn("arn:aws:iam::431047751999:instance-profile/GuyRole").build();

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_MICRO)
                    .iamInstanceProfile(role)
                    .imageId("ami-064b250192ddec4c3")
                    .keyName("DSP211")
                    .maxCount(1)
                    .minCount(1)
                    .userData(Base64.getEncoder().encodeToString("#!/bin/bash\ncd ~\nrm worker.jar\nwget https://files-guy-eden.s3.amazonaws.com/worker.jar\njava -jar worker.jar".getBytes()))
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);

            String instanceId = response.instances().get(0).instanceId();

            Tag tag = Tag.builder()
                    .key("Name")
                    .value("worker")
                    .build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            try {
                ec2.createTags(tagRequest);

            } catch (Ec2Exception e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }


    private static void deleteMessage(SqsClient sqsClient, String queueUrl,  Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }

    private static String createQueue(SqsClient sqsClient) {

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName("worker")
                .build();

        sqsClient.createQueue(createQueueRequest);

        GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName("worker").build());
        return getQueueUrlResponse.queueUrl();
    }

    private static void putInputObject (S3Client s3, String bucket, String str) {
        String [] parts = str.split(":",2);
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(parts[0])
                .build(), RequestBody.fromString(parts[1]));
    }
}

