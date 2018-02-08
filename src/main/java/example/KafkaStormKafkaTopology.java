package example;

import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.testing.IdentityBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class KafkaStormKafkaTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

        final TopologyBuilder builder = new TopologyBuilder();
        final Fields fields = new Fields("topic", "key", "message");

        // Properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "host-10-1-236-128:6667,host-10-1-236-129:6667,host-10-1-236-130:6667");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SASL_PLAINTEXT");

        /* do not need config jaas here in HDP 2.6.0.3 with STORM 1.1.0
        props.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required "
                    + "useTicketCache=false "
                    + "renewTicket=true "
                    + "serviceName=\"ocdp\" "
                    + "useKeyTab=true "
                    + "keyTab=\"/root/rainday/storm/kafka.keytab\" "
                    + "principal=\"ocdp/host-10-1-236-128@ASIAINFO.COM\";");
                    */

        // Kafka spout getting data from "foo"
        KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig
                .builder(props.getProperty("bootstrap.servers"), "foo")
                .setGroupId("KafkaStormGroupID")
                .setProp(props)
                .setRecordTranslator((r) -> new Values(r.topic(), r.key(), r.value()), new Fields("topic", "key", "message"))
                .build();


        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);

        // Identity bolt (just for testing, doing nothing)
        IdentityBolt identityBolt = new IdentityBolt(fields);

        // Kafka bolt to send data into "outputTopicStorm"
        KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("outputTopicStorm"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>());

        // Building the topology: KafkaSpout -> Identity -> KafkaBolt
        builder.setSpout("kafka-spout", kafkaSpout);
        builder.setBolt("identity", identityBolt).shuffleGrouping("kafka-spout");
        builder.setBolt("kafka-bolt", kafkaBolt, 2).globalGrouping("identity");

        // Submit the topology
        Config conf = new Config();
        StormSubmitter.submitTopology("Kafka-Storm-Kafka", conf, builder.createTopology());
    }
}
