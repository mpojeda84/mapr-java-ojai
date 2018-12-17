package com.mpojeda84.maprdb.java.ojai;

import com.mpojeda84.maprdb.java.ojai.model.Business;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.*;


public class Application {

    private static Connection connection;

    private static void initializeConnection() {
        connection = DriverManager.getConnection("ojai:mapr:");
        System.out.println("Connection opened");
    }

    private static void closeConnection() {
        connection.close();
        System.out.println("Connection closed");
    }

    private static void insertBusinesses() {

        final DocumentStore store = connection.getStore("/user/mapr/tables/business");
        for (int i = 0; i < 5; i++) {
            final Document document = connection.newDocument(new Business(Integer.toString(i), Integer.toString(i) + " Street, Fort Lauderdale", i % 2 == 0));
            store.insertOrReplace(document);
        }
        System.out.println("Inserted/Updated 5 Documents");
        store.close();
    }

    private static void printBusiness2() {
        final DocumentStore store = connection.getStore("/user/mapr/tables/business");
        Document document = store.findById("2");
        System.out.println("Business with id: 2 is --> " + document.asJsonString());
        store.close();

    }

    private static void printAllBusinesses() {
        System.out.println("All Business in table");
        final DocumentStore store = connection.getStore("/user/mapr/tables/business");
        final DocumentStream stream = store.find();
        stream.forEach(x -> System.out.println(x.asJsonString()));
        store.close();
    }

    private static void printOpenBusinesses() {
        System.out.println("All Open Business in table");
        final DocumentStore store = connection.getStore("/user/mapr/tables/business");

        final Query query = connection.newQuery()
                .where(
                        connection.newCondition()
                                .is("open", QueryCondition.Op.EQUAL, true)
                                .build())
                .build();

        final DocumentStream stream = store.find(query);
        stream.forEach(x -> System.out.println(x.asJsonString()));
        store.close();
    }

    private static void printClosedBusinesses() {
        System.out.println("All Closed Business in table");
        final DocumentStore store = connection.getStore("/user/mapr/tables/business");

        final Query query = connection.newQuery()
                .where("{\"$eq\": {\"open\": false}}")
                .build();

        final DocumentStream stream = store.find(query);
        stream.forEach(x -> System.out.println(x.asJsonString()));
        store.close();
    }

    // business table created previously in '/user/mapr/tables/'
    // run main with -Dmapr.home.dir set to MapR Client home dir
    // example: -Dmapr.home.dir=/opt/mapr

    public static void main(String[] args) {
        initializeConnection();
        System.out.println("--------------------");

        insertBusinesses();
        System.out.println("--------------------");

        printAllBusinesses();
        System.out.println("--------------------");

        printBusiness2();
        System.out.println("--------------------");

        printOpenBusinesses();
        System.out.println("--------------------");

        printClosedBusinesses();
        System.out.println("--------------------");

        closeConnection();
        System.out.println("--------------------");

    }
}
