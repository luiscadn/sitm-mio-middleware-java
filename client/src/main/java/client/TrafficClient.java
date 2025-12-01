package client;

import SITM.*;
import com.zeroc.Ice.*;

import java.util.Map;

public class TrafficClient implements TrafficSubscriber {

    @Override
    public void updateSpeeds(Map<Integer, Double> liveSpeeds, Current current) {
        System.out.println("=== UPDATE DE VELOCIDADES ===");
        liveSpeeds.forEach((route, speed) -> {
            System.out.printf("Ruta %d → %.2f km/h\n", route, speed);
        });
        System.out.println("=============================");
    }

    public static void main(String[] args) {
        try (Communicator ic = Util.initialize(args)) {

            // 1. Obtener proxy remoto del Master
            ObjectPrx base = ic.stringToProxy("Master:default -p 10000");
            MasterPrx master = MasterPrx.checkedCast(base);

            if (master == null) {
                System.out.println("Error: No se pudo obtener el Master.");
                return;
            }

            // 2. Crear un ObjectAdapter para recibir callbacks
            ObjectAdapter adapter = ic.createObjectAdapterWithEndpoints(
                    "TrafficAdapter",
                    "tcp -p 20000"
            );

            // 3. Registrar este cliente como objeto remoto
            TrafficSubscriberPrx proxy = TrafficSubscriberPrx.uncheckedCast(
                    adapter.addWithUUID(new TrafficClient())
            );

            adapter.activate();

            // 4. Registrar el subscriber en el Master
            master.registerTrafficSubscriber(proxy);

            System.out.println("Cliente listo para recibir actualizaciones de tráfico...");
            ic.waitForShutdown();
        }
    }
}
