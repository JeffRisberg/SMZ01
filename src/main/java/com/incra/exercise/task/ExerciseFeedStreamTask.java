package com.incra.exercise.task;

import com.incra.tutorial.CommonProtos;
import com.incra.tutorial.TrackingProtos;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.util.Map;

/**
 * This task is very simple. All it does is receive exercise data records on Kafka topic "exercise-raw" and sends
 * to a Kafka topic "exercise-processed".
 *
 * @author Jeff Risberg
 * @since 01/09/16
 */
public class ExerciseFeedStreamTask implements StreamTask, InitableTask {

    protected static final String STREAM_NAME_PROD_KAFKA = "kafka";
    protected static final SystemStream OUTPUT_STREAM = new SystemStream(STREAM_NAME_PROD_KAFKA, "exercise-processed");

    private static final String KV_STORE_NAME = "tracking_store";

    protected String taskName;
    protected KeyValueStore<String, Integer> kvStore;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        System.out.println("\nExerciseFeedStreamTask init");

        taskName = config.get("job.name", "UNKNOWN");

        kvStore = (KeyValueStore<String, Integer>) context.getStore(KV_STORE_NAME);

        clearAll(kvStore);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {

        System.out.println("\nbegin process incoming message");

        try {
            TrackingProtos.Tracking incomingTracking = (TrackingProtos.Tracking) envelope.getMessage();
            CommonProtos.Player player = incomingTracking.getPlayer();
            TrackingProtos.Activity activity = incomingTracking.getActivity();
            int amount = incomingTracking.getAmount();

            String activityName = activity.getName();
            Object currValueObj = kvStore.get(activityName);
            int currValue = 0;
            if (currValueObj != null) currValue = (Integer) currValueObj;

            int newValue = currValue + amount;
            kvStore.put(activityName, newValue);
            System.out.println("activity " + activityName + " sum " + newValue);

            TrackingProtos.Tracking.Builder outgoingTracking = TrackingProtos.Tracking.newBuilder();

            outgoingTracking.setPlayer(player);
            outgoingTracking.setActivity(activity);
            outgoingTracking.setAmount(newValue);
            outgoingTracking.setTrackingDate(System.currentTimeMillis());

            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outgoingTracking.build()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("end process incoming message");
    }

    private void clearAll(KeyValueStore<String, Integer> kvStore) {
        KeyValueIterator<String, Integer> itr = kvStore.all();
        try {
            while (itr.hasNext()) {
                Entry<String, Integer> entry = itr.next();
                kvStore.delete(entry.getKey());
            }
        } finally {
            if (itr != null) itr.close();
        }
    }
}
