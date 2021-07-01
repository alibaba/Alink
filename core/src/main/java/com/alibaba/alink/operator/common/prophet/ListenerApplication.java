package com.alibaba.alink.operator.common.prophet;

import py4j.GatewayServer;

import java.util.ArrayList;
import java.util.List;

/**
 * the class that python code invoke
 */
public class ListenerApplication {
    private static List<ExampleListener> listeners;
    private static GatewayServer server;
    public static String modelParam;
    public static String returnValue;

    public static void registerListener(ExampleListener listener) {
        listeners.add(listener);
    }

    public static void notifyAllListeners() {
        for (ExampleListener listener: listeners) {
            Object returned = listener.notify(modelParam);
            returnValue = String.valueOf(returned);
        }
    }

    public static void open(){
        ListenerApplication application = new ListenerApplication();
        server = new GatewayServer(application);
        listeners = new ArrayList<>();
        server.start(true);
    }

    public static void close(){
        server.shutdown();
    }

    @Override
    public String toString() {
        return "<ListenerApplication> instance";
    }
}
