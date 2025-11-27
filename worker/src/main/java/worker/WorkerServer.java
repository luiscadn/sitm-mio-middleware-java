package worker;

import SITM.*;
import com.zeroc.Ice.*;

public class WorkerServer {
    public static void main(String[] args) {
        try (Communicator communicator = Util.initialize(args, "config.worker")) {
            ObjectAdapter adapter = communicator.createObjectAdapter("Worker");

            WorkerImpl workerImpl = new WorkerImpl(communicator);

            ObjectPrx objectPrx = adapter.add(workerImpl, Util.stringToIdentity("worker"));
            
            adapter.activate();
            System.out.println("Worker iniciado y escuchando...");

            String masterProxyStr = communicator.getProperties().getProperty("Master.Proxy");
            if (masterProxyStr == null) {
                System.err.println("Error: Propiedad Master.Proxy no definida en config.worker");
                return;
            }

            MasterPrx master = MasterPrx.checkedCast(communicator.stringToProxy(masterProxyStr));
            if (master == null) {
                System.err.println("Error: Proxy del Master invalido");
                return;
            }

            WorkerPrx workerPrx = WorkerPrx.uncheckedCast(objectPrx);
            
            System.out.println("Registrándose en el Master...");
            master.registerWorker(workerPrx);
            System.out.println("¡Registro exitoso! Esperando tareas...");

            communicator.waitForShutdown();
            
        } catch (java.lang.Exception e) { 
            e.printStackTrace();
        }
    }
}