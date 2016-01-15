package com.incra.exercise.task;

import com.incra.tutorial.TrackingProtos;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.Map;

/**
 * This task is very simple. All it does is receive exercise data and send them to a Kafka topic
 * called exercise-raw.
 *
 * @author Jeff Risberg
 * @since 01/09/16
 */
public class ExerciseFeedStreamTask implements StreamTask {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "exercise-processed");

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {

        try {
            byte[] bytes = (byte[]) envelope.getMessage();
            TrackingProtos.Tracking incomingTracking = TrackingProtos.Tracking.parseFrom(bytes);

            TrackingProtos.Tracking.Builder outgoingTracking = TrackingProtos.Tracking.newBuilder();

            outgoingTracking.setActivity(incomingTracking.getActivity());
            outgoingTracking.setAmount(incomingTracking.getAmount());

            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outgoingTracking.build().toByteArray()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
