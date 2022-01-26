package io.pravega.controller.server;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteArraySerializer;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.Random;


public class Write {

    private static int BYTES_STEP = 1000;
    
    private static long TOTAL_BYTES_MB = 60L;
    
    //private static int ONE_MILLION = 1000000;

    //    static {
    //        MetricsProvider.initialize(MetricsConfig.builder().);
    //    }
    //    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    public static void testWrite() throws InterruptedException, IOException {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .scalingPolicy(ScalingPolicy.fixed(1))
                                                              .build();
        URI controllerURI = URI.create("tcp://localhost:9090");

        try (StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamManager.createScope("testscope");
            streamManager.createStream("testscope", "teststream", streamConfig);
        }
        ClientConfig clientConfig = ClientConfig.builder()
                                                .controllerURI(controllerURI).build();
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamClientFactory factory = EventStreamClientFactory
                .withScope("testscope", clientConfig);
        EventStreamWriter<byte[]> writer = factory
                .createEventWriter("teststream", new ByteArraySerializer(), writerConfig);
        Random rd = new Random();
        System.out.println("Begin writing");
        byte[] arr;
        boolean pause_tier2 = false;
        int num_events = 0;
        int operations = 1;
        long bytes_written = 0;
        while (true) {
            int i = 0;

            while (++i <= 10) {
                arr = new byte[BYTES_STEP];
                rd.nextBytes(arr);
//                writer.writeEvent("rk", arr);
                writer.writeEvent(arr);
                operations++;
                num_events++;
                if(!pause_tier2){
                   System.out.println("pausing tier 2");
                   Runtime.getRuntime().exec("docker pause a93c50ac8cb6");
                   Thread.sleep(1000);
                   pause_tier2=!pause_tier2;
                   System.out.println("begin writing at "+LocalDateTime.now().toString());
                }else {
                    bytes_written += BYTES_STEP;
                }
                //DYNAMIC_LOGGER.reportGaugeValue("pravega.newclient.bytes.written", bytes_written, "client_bytes" );
            }
            writer.flush();
//            if (bytes_written >= 1000 * 1000 * TOTAL_BYTES_MB) {
//                System.out.println("written 200mb at "+LocalDateTime.now().toString());
//                break;
//            }
            System.out.println("bytes written " + bytes_written + "at " + LocalDateTime.now().toString());
        }
//        System.out.println("resume tier2");
//        System.out.println("no of events written "+num_events);
//        Runtime.getRuntime().exec("docker unpause 0bbe904dda8a");
    }

        public static void main(String[] args) throws InterruptedException, IOException{

            testWrite();
        }
}
