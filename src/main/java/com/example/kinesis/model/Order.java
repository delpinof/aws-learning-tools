package com.example.kinesis.model;

import lombok.Data;

@Data
public class Order {
    int orderId;
    String product;
    int quantity;
}
