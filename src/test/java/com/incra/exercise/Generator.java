package com.incra.exercise;

import com.incra.tutorial.CommonProtos;
import com.incra.tutorial.TrackingProtos;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Updated version of publisher test that uses Kafka 0.9.x to publish Operations, with
 * content encoded by Protocol Buffers.
 *
 * @author Jeff Risberg
 * @since 01/10/16
 */
public class Generator {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // the leader will wait for the full set of in-sync replicas to acknowledge the record.
        // This guarantees that the record will not be lost as long as at least one in-sync replica
        // remains alive.
        props.put(ACKS_CONFIG, "all");

        // don't retry automatically since this might change the message ordering.
        props.put(RETRIES_CONFIG, 0);

        // batch records for 1 ms.
        props.put(LINGER_MS_CONFIG, 1);
        // linger.ms is ignored when there are >= 16384 records
        props.put(BATCH_SIZE_CONFIG, 16384);

        props.put(BUFFER_MEMORY_CONFIG, 33554432);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        props.put(CLIENT_ID_CONFIG, "generator-test");

        Producer<String, byte[]> producer = new KafkaProducer<>(props);
        String topic = "exercise-raw";
        String msgKey = "a";

        for (int i = 0; i < 10; i++) {
            System.out.println("Sending: " + i);

            try {
                TrackingProtos.Tracking tracking = createMessage();

                System.out.println(tracking);
                byte[] msgContent = tracking.toByteArray();
                System.out.println(msgContent);

                producer.send(new ProducerRecord<>(topic, msgKey, msgContent)).get();
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }

    static TrackingProtos.Tracking createMessage() throws IOException {
        int amount = (int) (Math.random() * 15);
        TrackingProtos.Tracking.Builder message = TrackingProtos.Tracking.newBuilder();

        TrackingProtos.Activity.Builder activity = TrackingProtos.Activity.newBuilder();
        activity.setName("Running");
        activity.setId(1);
        activity.setUom("km");

        CommonProtos.Player.Builder player = CommonProtos.Player.newBuilder();
        player.setId(1);
        player.setName("Winston Smith");
        player.setEmail("winston@minitru.com");

        message.setActivity(activity.build());
        message.setPlayer(player.build());
        message.setAmount(amount);
        message.setTrackingDate(System.currentTimeMillis());

        return message.build();
    }
}
