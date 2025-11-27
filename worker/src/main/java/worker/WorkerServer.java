package worker;

import SITM.*;
import com.zeroc.Ice.*;

public class WorkerServer {
    public static void main(String[] args) {
        String configFile = "config.worker";
        for (String arg : args) {
            if (arg.startsWith("--config=")) {
                configFile = arg.split("=")[1];
                System.out.println("[System] Usando configuración personalizada: " + configFile);
            }
        }

        try (Communicator communicator = Util.initialize(args, configFile)) {
            ObjectAdapter adapter = communicator.createObjectAdapter("Worker");

            WorkerImpl workerImpl = new WorkerImpl(communicator);
            ObjectPrx objectPrx = adapter.add(workerImpl, Util.stringToIdentity("worker"));
            
            adapter.activate();
            
            System.out.println("Worker iniciado con config: " + configFile);

            String masterProxyStr = communicator.getProperties().getProperty("Master.Proxy");
            if (masterProxyStr == null) {
                System.err.println("Error: Propiedad Master.Proxy no definida");
                return;
            }

            MasterPrx master = MasterPrx.checkedCast(communicator.stringToProxy(masterProxyStr));
            if (master == null) {
                System.err.println("Error: Proxy del Master invalido");
                return;
            }

            WorkerPrx workerPrx = WorkerPrx.uncheckedCast(objectPrx);
            master.registerWorker(workerPrx);
            System.out.println("¡Registro exitoso! Esperando tareas...");

            communicator.waitForShutdown();
            
        } catch (java.lang.Exception e) {
            e.printStackTrace();
        }
    }
}