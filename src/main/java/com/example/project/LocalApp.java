package com.example.project;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import static java.lang.System.exit;

public class LocalApp {

    static String inputFileName;
    static String outputFileName;
    static String localURL;
    static String n;
    static S3Client s3;
    static String localBucket;
    static SqsClient sqsClient;
    public static void main (String [] args) throws IOException, InterruptedException {

        //Get the arguments
        System.out.println("Local is up, please wait\n");
        try {
            inputFileName = args[0];
            File f = new File(inputFileName); //checking if file exist
            if (!f.exists()){
                System.out.println("Input file does not exist. Please load the app again.");
                exit (1);
            }
            outputFileName = args[1];
            n = args[2]; //assuming that the arguments are right
        }
        catch (Exception e){
            System.out.println("can't read arguments as expected.\ncheck your arguments and load the app again.");
            exit (1);
        }

        String Delimiter = "HereComesDelimiter"; //our delimiter for sending massage to manager's queue.

        //create SQS queue for local. Unique name by current date format.

        sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        Date date1 = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss");
        String sqsName = "local" + formatter.format(date1); //for unique name, we add the current time for branch name
        localURL = createQueue(sqsClient, sqsName);

        //check if manager is active, if not, create instance and queue for it.

        s3 = S3Client.builder().region(Region.US_EAST_1).build();
        boolean isActive = false;
        String managerURL = "";
        List <Bucket> buckets = s3.listBuckets().buckets();
        if(buckets.get(0) != null ){
            ListObjectsRequest request = ListObjectsRequest.builder().bucket(buckets.get(0).name()).build();
            ListObjectsResponse objectList = s3.listObjects(request);
            for(S3Object s3Object: objectList.contents()) {
                String key = s3Object.key();
                if (key.equals("manager")) {
                    isActive = true;
                    managerURL = getObject(buckets.get(0).name());
                }
            }
        }

        if (!isActive) {
            String bucketForManager = createBucket(s3);
            managerURL = createQueue(sqsClient, "manager");
            putInputObject(s3, bucketForManager, "manager:"+managerURL);
            CreateManager();
        }

        //create bucket for local

        localBucket = createBucket(s3);

        //put in S3 the local URL and n value

        putInputObject(s3, localBucket, "n:" + n);
        putInputObject(s3, localBucket, "local:" + localURL);

        //put in S3 the input file.

        String location = putInputObject(s3, localBucket, new File(inputFileName));

        //send a new task to manager URL

        sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(managerURL).messageBody("new task"+Delimiter+location+Delimiter+localBucket).build());

        //waiting for massages

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(localURL)
                .maxNumberOfMessages(1)
                .visibilityTimeout(20)
                .build();
        Message message;
        java.util.List<Message> messages = null;
        int size = 0;
        while (size==0){
            messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            size = messages.size();
        }
        message = messages.get(0);
        boolean finish = false;

        while (!finish) {

            if (message.body().equals("done task")) {
                System.out.println("Got a summary file.\nCreating the html output");
                finish = true;

                //convert inputStream to String

                InputStream summary;
                summary = s3.getObject(GetObjectRequest.builder().bucket(localBucket).key("SummaryFile").build());

                String text = new BufferedReader(
                        new InputStreamReader(summary, StandardCharsets.UTF_8)).lines()
                        .collect(Collectors.joining("\n"));

                //creating html output

                try {
                    FileWriter myWriter = new FileWriter(outputFileName+".html");
                    myWriter.write(text);
                    myWriter.close();
                } catch (IOException e) {
                    System.out.println("An error occurred.");
                    exit(1);
                }
                System.out.println("Your output file is created successfully\n");
                String current = new java.io.File( "." ).getCanonicalPath();
                System.out.println("You can find your output in folder: "+ current+"\n");

                //delete local bucket

                deleteBucket ();

                //delete local sqs

                sqsClient.deleteQueue(DeleteQueueRequest.builder().queueUrl(localURL).build());

                //check if we got terminate as argument, if yes, send terminate massage to manager's queue.
                if (args.length > 3 && args [3].equals("terminate"))
                    sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(managerURL).messageBody("terminateHereComesDelimiter").build());
            }
        }
    }

    private static String createQueue(SqsClient sqsClient, String queueName ) {

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        try {
            sqsClient.createQueue(createQueueRequest);
        }
        catch (QueueDeletedRecentlyException e){
            System.out.println("The manager has just been terminated, please wait 60 sec and try again");
            sqsClient.deleteQueue(DeleteQueueRequest.builder().queueUrl(localURL).build());
            deleteBucket ();
            exit(1);
        }catch (SqsException e){
            System.out.println("Please update your credentials");
            exit(1);
        }

        GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        return getQueueUrlResponse.queueUrl();
    }

    private static String createBucket(S3Client s3) {
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss");
        String bucket = "eden-guy-ocr-" + formatter.format(date); //for unique name, we add the current time for branch name
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucket)
                    .build());
        }
        catch (NoSuchBucketException e){
            exit (1);
        }
        return bucket;
    }

    private static String putInputObject (S3Client s3, String bucket, File file) {
        String key = "OCRFile";
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                .build(), RequestBody.fromFile(file));
        return key;
    }

    private static void putInputObject (S3Client s3, String bucket, String str) {
        String [] parts = str.split(":",2);
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(parts[0])
                .build(), RequestBody.fromString(parts[1]));
    }

    private static void CreateManager (){
        Ec2Client ec2 = Ec2Client.create();

        IamInstanceProfileSpecification role = IamInstanceProfileSpecification.builder().arn("arn:aws:iam::431047751999:instance-profile/GuyRole").build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .iamInstanceProfile(role)
                .imageId("ami-064b250192ddec4c3")
                .keyName("DSP211")
                .maxCount(1)
                .minCount(1)
                .userData(Base64.getEncoder().encodeToString("#!/bin/bash\ncd ~\nrm manager.jar\nwget https://files-guy-eden.s3.amazonaws.com/manager.jar\njava -jar manager.jar".getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value("manager")
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

    private static String getObject(String bucket) throws IOException {
        ResponseInputStream<GetObjectResponse> object = s3.getObject(GetObjectRequest.builder().bucket(bucket).key("manager").build());
        BufferedReader reader = new BufferedReader(new InputStreamReader(object));
        String line;
        StringBuilder finish = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            finish.append(line);
        }
        return finish.toString();
    }

    private static void deleteBucket (){
        ListObjectsRequest request = ListObjectsRequest.builder().bucket(localBucket).build();
        ListObjectsResponse objectList = s3.listObjects(request);
        for(S3Object s3Object: objectList.contents()) {
            String key = s3Object.key();
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(localBucket).key(key).build();
            s3.deleteObject(deleteObjectRequest);
        }
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(localBucket).build();
        s3.deleteBucket(deleteBucketRequest);
    }
}
