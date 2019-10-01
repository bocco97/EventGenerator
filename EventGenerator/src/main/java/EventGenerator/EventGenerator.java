package EventGenerator;

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

    public static void main(String[] args) {
        long interval = 0;
        long MIN_INTERVAL = 0;
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
            interval = Long.parseLong(prop.getProperty("INTERVAL"));
            if (interval < MIN_INTERVAL){
                throw new NumberFormatException();
            }
        }
        catch(NumberFormatException e){
            System.out.println("INTERVAL in config.properties must be an Int >= "+MIN_INTERVAL);
            return;
        }
        //lista da cui estrarre casualmente il campo recclass per l'evento
        String path_to_keys_file = prop.getProperty("RECCLASS_FILE");
        if (path_to_keys_file==null){
            System.out.println("Missing RECCLASS_FILE in config.properties");
            return;
        }
        List<String> keys;
        try{
            keys = extractKeys(path_to_keys_file);
        }
        catch (FileNotFoundException e){
            System.out.println("RECCLASS_FILE not found");
            return;
        }
        while(true){
            String lat = randomGeographicValue();
            String lon = randomGeographicValue();
            int mass = rand.nextInt(200000);
            String type = randomPick(keys);
            String date = java.time.LocalDateTime.now().toString();

            System.out.println("lat: "+lat+" lon: "+lon + " mass: "+mass+" recclass: "+type+ " date: "+date);

            try {
                sleep(interval);
            }
            catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
