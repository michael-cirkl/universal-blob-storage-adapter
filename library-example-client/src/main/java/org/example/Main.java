package org.example;


import michaelcirkl.ubsa.WrapperFactory;
import michaelcirkl.ubsa.testclients.BlobStoreTest;

public class Main {
    public static void main(String[] args) {
        System.out.println("hi");

        BlobStoreTest wrapper = WrapperFactory.getWrapper();
        System.out.println(wrapper.getClass().getSimpleName());
        wrapper.test();
    }
}
