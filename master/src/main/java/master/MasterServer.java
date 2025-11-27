package master;

import com.zeroc.Ice.*;

public class MasterServer {
    public static void main(String[] args) {
        try (Communicator communicator = Util.initialize(args, "config.master")) {
            
            ObjectAdapter adapter = communicator.createObjectAdapter("MasterAdapter");

            MasterImpl masterImpl = new MasterImpl(communicator);

            adapter.add(masterImpl, Util.stringToIdentity("Master"));

            adapter.activate();
            
            masterImpl.startManifestWatcher();

            System.out.println("[Master] Listo y escuchando en el puerto 10000...");
            
            communicator.waitForShutdown();
            
        } catch (java.lang.Exception e) {
            e.printStackTrace();
        }
    }
}