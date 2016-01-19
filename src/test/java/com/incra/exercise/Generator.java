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
        List<TrackingProtos.Activity> activities = new ArrayList<TrackingProtos.Activity>();

        TrackingProtos.Activity.Builder activity1 = TrackingProtos.Activity.newBuilder();
        activity1.setName("Hiking");
        activity1.setId(1);
        activity1.setUom("km");
        activities.add(activity1.build());

        TrackingProtos.Activity.Builder activity2 = TrackingProtos.Activity.newBuilder();
        activity2.setName("Walking");
        activity2.setId(2);
        activity2.setUom("miles");
        activities.add(activity2.build());

        TrackingProtos.Activity.Builder activity3 = TrackingProtos.Activity.newBuilder();
        activity3.setName("Running");
        activity3.setId(3);
        activity3.setUom("km");
        activities.add(activity3.build());

        TrackingProtos.Activity.Builder activity4 = TrackingProtos.Activity.newBuilder();
        activity4.setName("Dancing");
        activity4.setId(4);
        activity4.setUom("hours");
        activities.add(activity4.build());

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
                TrackingProtos.Tracking tracking = createMessage(activities);

                System.out.println(tracking);
                byte[] msgContent = tracking.toByteArray();

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

    static TrackingProtos.Tracking createMessage(List<TrackingProtos.Activity> activities) throws IOException {
        TrackingProtos.Activity activity = activities.get((int) (activities.size() * Math.random()));

        int amount = 3 + (int) (Math.random() * 15);
        TrackingProtos.Tracking.Builder message = TrackingProtos.Tracking.newBuilder();

        CommonProtos.Player.Builder player = CommonProtos.Player.newBuilder();
        player.setId(1);
        player.setName("Winston Smith");
        player.setEmail("winston@minitru.com");

        message.setActivity(activity);
        message.setPlayer(player.build());
        message.setAmount(amount);
        message.setTrackingDate(System.currentTimeMillis());

        return message.build();
    }
}
