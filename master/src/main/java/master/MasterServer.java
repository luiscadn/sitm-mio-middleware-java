package master;

import com.zeroc.Ice.*;
import org.apache.activemq.broker.BrokerService;

public class MasterServer {
    public static void main(String[] args) {
        BrokerService broker = new BrokerService();
        try {
            // 1. Iniciar el broker de ActiveMQ embebido
            broker.addConnector("tcp://localhost:61616");
            broker.setPersistent(false); // Usamos una cola no persistente para simplificar
            broker.start();
            System.out.println("[Master] Broker de mensajería iniciado.");

            // 2. Iniciar el servidor Ice
            try (Communicator communicator = Util.initialize(args, "config.master")) {
                ObjectAdapter adapter = communicator.createObjectAdapter("MasterAdapter");
                MasterImpl masterImpl = new MasterImpl(communicator);
                adapter.add(masterImpl, Util.stringToIdentity("Master"));
                adapter.activate();
                masterImpl.startManifestWatcher();
                System.out.println("[Master] Listo y escuchando en el puerto 10000...");
                communicator.waitForShutdown();
            }
        } catch (java.lang.Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (broker.isStarted()) {
                    broker.stop();
                    System.out.println("[Master] Broker de mensajería detenido.");
                }
            } catch (java.lang.Exception e) {
                e.printStackTrace();
            }
        }
    }
}