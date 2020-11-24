package com.example.kinesis;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.example.kinesis.model.Order;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class Producer {

    private static List<String> productList = new ArrayList<>();
    private static Random random = new Random();
    private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static void main(String[] args) throws InterruptedException {
        populateList();
        new Producer().run();
    }

    public void run() throws InterruptedException {
        AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

        while (true) {
            List<PutRecordsRequestEntry> requestEntryList = getRecordsRequestList();
            PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
            putRecordsRequest.setStreamName("fherdelpino-test-datastream");
            putRecordsRequest.setRecords(requestEntryList);
            PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
            if (putRecordsResult.getFailedRecordCount() > 0 ) {
                log.error("{} Failed records produced", putRecordsResult.getFailedRecordCount());
            } else {
                log.info("Data produced successfully");
            }
            Thread.sleep(5000);
        }

    }

    private List<PutRecordsRequestEntry> getRecordsRequestList() {
        return getOrderList()
                .stream()
                .map(this::transform)
                .collect(Collectors.toList());
    }

    private PutRecordsRequestEntry transform(Order order) {
        PutRecordsRequestEntry requestEntry = new PutRecordsRequestEntry();
        requestEntry.setData(ByteBuffer.wrap(gson.toJson(order).getBytes()));
        requestEntry.setPartitionKey(UUID.randomUUID().toString());
        return requestEntry;
    }

    private List<Order> getOrderList() {
        List<Order> orders = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            orders.add(produceOrder());
        }
        return orders;
    }

    private Order produceOrder() {
        Order order = new Order();
        order.setOrderId(random.nextInt());
        order.setProduct(productList.get(random.nextInt(productList.size())));
        order.setQuantity(random.nextInt(20));
        return order;
    }

    private static void populateList() {
        productList.add("shirt");
        productList.add("t-shirt");
        productList.add("short");
        productList.add("tie");
        productList.add("shoes");
        productList.add("jeans");
        productList.add("belt");
    }
}
