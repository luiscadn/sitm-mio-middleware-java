package client;

import SITM.*;
import com.zeroc.Ice.*;
import java.io.*;
import java.util.*;

public class StreamClient {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Uso: StreamClient <csvFile>");
            return;
        }

        try (Communicator communicator = Util.initialize(args, "config.client")) {
            String proxyStr = communicator.getProperties().getProperty("Master.Proxy");
            if (proxyStr == null) proxyStr = "Master:tcp -h localhost -p 10000"; // Fallback
            
            MasterPrx master = MasterPrx.checkedCast(communicator.stringToProxy(proxyStr));
            if (master == null) throw new Error("Master invalido");

            System.out.println("Conectado al Master. Iniciando transmisión de datos en vivo...");

            try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
                String line;
                List<String> batch = new ArrayList<>();
                int batchSize = 100; 
                
                br.readLine(); 

                while ((line = br.readLine()) != null) {
                    batch.add(line);

                    if (batch.size() >= batchSize) {
                        master.ingestStream(batch.toArray(new String[0]));
                        System.out.print("."); 
                        
                        batch.clear();
                        
                        Thread.sleep(100); 
                    }
                }
                if (!batch.isEmpty()) master.ingestStream(batch.toArray(new String[0]));
                
                System.out.println("\n[StreamClient] Transmisión finalizada.");

            } catch (java.lang.Exception e) { 
                e.printStackTrace();
            }
        }
    }
}