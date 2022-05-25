/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.clebert.reproducer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;

/**
 * A simple JMS Queue example that creates a producer and consumer on a queue and sends then receives a message.
 */
public class QueueReproducer {

   static int REPEATS = 100;
   static int NUMBER_OF_CONNECTIONS = 5;
   static int NUMBER_OF_SESSIONS = 100;


   static String uri = "localhost:61616";
   static ExecutorService executorService;

   public static void main(final String[] args) throws Exception {
      try {
         for (int r = 0; r < REPEATS; r++) {
            System.out.println("*******************************************************************************************************************************");
            System.out.println("Core Protocol running");
            ConnectionFactory coreConnectionFactory = new ActiveMQConnectionFactory("tcp://" + uri);
            doRun(coreConnectionFactory);
            System.out.println("AMQP running");
            ConnectionFactory amqpConnectionFactory = new JmsConnectionFactory("amqp://" + uri);
            doRun(amqpConnectionFactory);

            System.out.println("OpenWire running");
            ConnectionFactory openConnectionFactory = new org.apache.activemq.ActiveMQConnectionFactory("tcp://" + uri);
            doRun(amqpConnectionFactory);
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public static void runSession(Connection connection, CyclicBarrier barrier, CountDownLatch latch) {
      try {
         barrier.await(10, TimeUnit.SECONDS);
         Session session = connection.createSession();
         MessageConsumer consumer = session.createConsumer(session.createQueue("Test"));
         session.close();
      } catch(Exception e) {
         e.printStackTrace();
      } finally {
         latch.countDown();
      }
   }


   public static void doRun(ConnectionFactory factory) {
      CountDownLatch latch = new CountDownLatch(NUMBER_OF_CONNECTIONS * NUMBER_OF_SESSIONS);
      executorService = Executors.newFixedThreadPool(NUMBER_OF_CONNECTIONS * NUMBER_OF_SESSIONS);
      final CyclicBarrier barrier = new CyclicBarrier(NUMBER_OF_CONNECTIONS * NUMBER_OF_SESSIONS);
      final Connection[] connections = new Connection[NUMBER_OF_CONNECTIONS];
      try {
         System.out.println("Creating connections");
         for (int i = 0; i < connections.length; i++) {
            connections[i] = factory.createConnection();
            connections[i].start();
            final Connection connectionInUse = connections[i];
            for (int j = 0; j < NUMBER_OF_SESSIONS; j++) {
               executorService.execute(() -> runSession(connectionInUse, barrier, latch));
            }
         }
         System.out.println("Awaiting termination");
         latch.await(1, TimeUnit.MINUTES);
         System.out.println("Done");
         executorService.shutdownNow();
         executorService.awaitTermination(1, TimeUnit.MINUTES);
         for (Connection c : connections) {
            c.close();
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}