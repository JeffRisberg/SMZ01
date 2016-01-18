package com.incra.exercise.task;

import com.incra.tutorial.CommonProtos;
import com.incra.tutorial.TrackingProtos;
import org.apache.samza.config.Config;
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

    protected String taskName;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        System.out.println("\nExerciseFeedStreamTask init");
        System.err.println("\nExerciseFeedStreamTask init");

        taskName = config.get("job.name", "UNKNOWN");
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {

        System.out.println("\nbegin process incoming message");
        System.err.println("\nbegin process incoming message");
        try {
            TrackingProtos.Tracking incomingTracking = (TrackingProtos.Tracking) envelope.getMessage();
            CommonProtos.Player player = incomingTracking.getPlayer();
            TrackingProtos.Activity activity = incomingTracking.getActivity();
            int amount = incomingTracking.getAmount();

            TrackingProtos.Tracking.Builder outgoingTracking = TrackingProtos.Tracking.newBuilder();

            outgoingTracking.setPlayer(player);
            outgoingTracking.setActivity(activity);
            outgoingTracking.setAmount(100 + amount);
            outgoingTracking.setTrackingDate(System.currentTimeMillis());

            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outgoingTracking.build()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("end process incoming message");
        System.err.println("end process incoming message");
    }
}
