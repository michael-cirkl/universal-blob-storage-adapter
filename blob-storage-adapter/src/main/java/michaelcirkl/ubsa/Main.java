package michaelcirkl.ubsa;

import michaelcirkl.ubsa.testclients.AWSTest;
import michaelcirkl.ubsa.testclients.AzureTest;
import michaelcirkl.ubsa.testclients.GCPTest;

public class Main {
    public static void main(String[] args) {
        new AWSTest().test();
        new AzureTest().test();
        new GCPTest().test();


    }





}