package EventGenerator;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Properties;
import static java.lang.Thread.sleep;

public class EventGenerator {

    //returns a random element of l
    private static <T>T randomPick(List<T> l){
        int max = l.size();
        Random rand = new Random();
        return l.get(rand.nextInt(max));
    }

    //returns -1 or 1, randomly
    private static int randomSign(){
        int seed =(int) (Math.random()*100);
        if(seed %2 ==0) return 1;
        else return -1;
    }

    //returns a random value for lat/lon
    private static String randomGeographicValue(){
        Random rand = new Random() ;
        int v1 = rand.nextInt(180)*randomSign();
        return v1+"."+rand.nextInt(99999);
    }

    //returns a list of string, extracted from the given file
    private static List<String> extractKeys(String path) throws FileNotFoundException{
        FileReader fi = new FileReader(path);
        BufferedReader br = new BufferedReader(fi);
        LinkedList<String> l = new LinkedList<>();
        try {
            String k = br.readLine();
            while (k != null) {
                l.add(k);
                k=br.readLine();
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return l;
    }

    private static KafkaProducer<String, String> createKafkaProducer(String server){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) {
        /*************************************** Parametri di configurazione ***********************************/
        long INTERVAL ;
        long MIN_INTERVAL = 0;
        String PATH_TO_KEYS_FILE;
        String KAFKA_SERVER;
        String KAFKA_TOPIC;
        /******************************************************************************************************/
        Random rand = new Random();
        Properties prop;
        try(InputStream input = new FileInputStream("config.properties")) {
            prop = new Properties();
            prop.load(input);
        }
        catch (FileNotFoundException e){
            System.out.println("config.properties file not found in "+ Paths.get("").toAbsolutePath().toString());
            return;
        }
        catch (IOException e){
            e.printStackTrace();
            return;
        }
        try {
            MIN_INTERVAL = Long.parseLong(prop.getProperty("MIN_INTERVAL"));
            INTERVAL = Long.parseLong(prop.getProperty("INTERVAL"));
            if (INTERVAL < MIN_INTERVAL){
                throw new NumberFormatException();
            }
        }
        catch(NumberFormatException e){
            System.out.println("INTERVAL in config.properties must be an Int >= "+MIN_INTERVAL);
            return;
        }
        //lista da cui estrarre casualmente il campo recclass per l'evento
        PATH_TO_KEYS_FILE = prop.getProperty("RECCLASS_FILE");
        if (PATH_TO_KEYS_FILE==null){
            System.out.println("Missing RECCLASS_FILE in config.properties");
            return;
        }
        KAFKA_SERVER = prop.getProperty("KAFKA_SERVER");
        if (KAFKA_SERVER==null){
            System.out.println("Missing KAFKA_SERVER in config.properties");
            return;
        }
        KAFKA_TOPIC = prop.getProperty("KAFKA_TOPIC");
        if (KAFKA_TOPIC==null){
            System.out.println("Missing KAFKA_TOPIC in config.properties");
            return;
        }
        List<String> keys;
        try{
            keys = extractKeys(PATH_TO_KEYS_FILE);
        }
        catch (FileNotFoundException e){
            System.out.println("RECCLASS_FILE not found");
            return;
        }

        KafkaProducer<String,String> producer = createKafkaProducer(KAFKA_SERVER);

        while(true){
            String lat = randomGeographicValue();
            String lon = randomGeographicValue();
            int mass = rand.nextInt(200000);
            String type = randomPick(keys);
            String date = java.time.LocalDateTime.now().toString();
            String event = "lat: " + lat + "\t\tlon: " + lon + "\t\tmass: " + mass + "\t\trecclass: " + type + "\t\t\tdate: " + date;

            ProducerRecord<String,String> record = new ProducerRecord<>(KAFKA_TOPIC,event);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e==null){
                        System.out.println("Correctly sent event: "+event);
                    }
                    else{
                        System.out.println("Error sending "+event);
                    }
                }
            });
            //System.out.println(event);

            try {
                sleep(INTERVAL);
            }
            catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
