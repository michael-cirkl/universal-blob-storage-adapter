package org.example;


import michaelcirkl.ubsa.BlobStorageClientBuilder;
import michaelcirkl.ubsa.BuilderFactory;
import michaelcirkl.ubsa.testclients.BlobStoreTest;

public class Main {
    public static void main(String[] args) {
        System.out.println("hi");

        BlobStorageClientBuilder builder = BuilderFactory.getClientBuilder();
        System.out.println(BuilderFactory.getCurrentProvider());
    }
}
