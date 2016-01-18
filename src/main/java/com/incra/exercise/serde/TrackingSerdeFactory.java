package com.incra.exercise.serde;

import com.incra.tutorial.TrackingProtos;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

/**
 * Defines the conversion of content into tracking messages.  This factory is registered with Samza at setup time.
 *
 * @author Jeff Risberg
 * @since 01/15/16
 */
public class TrackingSerdeFactory implements SerdeFactory<TrackingProtos.Tracking> {

    @Override
    public Serde<TrackingProtos.Tracking> getSerde(String s, Config config) {
        System.out.println("call to getSerde");

        return new Serde<TrackingProtos.Tracking>() {
            @Override
            public TrackingProtos.Tracking fromBytes(byte[] bytes) {
                try {
                    return TrackingProtos.Tracking.parseFrom(bytes);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public byte[] toBytes(TrackingProtos.Tracking event) {
                return event.toByteArray();
            }
        };
    }
}
