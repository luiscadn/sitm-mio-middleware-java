package worker;

import SITM.*;
import com.zeroc.Ice.*;

// Imports para JMS y ActiveMQ
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

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
            // --- REGISTRO EN ICE (se mantiene para reportar resultados) ---
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
            System.out.println("¡Registro en Ice exitoso!");

            // --- CONSUMIDOR DE MENSAJES JMS ---
            javax.jms.ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
            javax.jms.Connection connection = connectionFactory.createConnection();
            connection.start();

            // Usamos CLIENT_ACKNOWLEDGE para tener control total sobre el ACK
            javax.jms.Session session = connection.createSession(false, javax.jms.Session.CLIENT_ACKNOWLEDGE);
            javax.jms.Destination destination = session.createQueue("SITM_task_queue");
            javax.jms.MessageConsumer consumer = session.createConsumer(destination);

            consumer.setMessageListener(message -> {
                if (message instanceof javax.jms.MapMessage) {
                    try {
                        javax.jms.MapMessage mapMessage = (javax.jms.MapMessage) message;
                        int jobId = mapMessage.getInt("jobId");
                        int chunkId = mapMessage.getInt("chunkId");
                        String filePath = mapMessage.getString("filePath");
                        long startOffset = mapMessage.getLong("startOffset");
                        long dataSize = mapMessage.getLong("dataSize");

                        // Llamamos a la lógica de procesamiento
                        workerImpl.processChunk(jobId, chunkId, filePath, startOffset, dataSize);

                        // ¡CRÍTICO! Acusamos recibo solo si todo fue bien.
                        message.acknowledge();

                    } catch (javax.jms.JMSException e) {
                        System.err.println("[Worker-JMS] Error al procesar mensaje: " + e.getMessage());
                        // No hacemos ACK, el mensaje se re-entregará.
                    }
                }
            });

            System.out.println("[Worker-JMS] Escuchando la cola de tareas 'SITM_task_queue'...");

            // El servidor se mantiene activo esperando callbacks de Ice y mensajes de JMS
            communicator.waitForShutdown();

            // Cerrar recursos de JMS al finalizar
            consumer.close();
            session.close();
            connection.close();

        } catch (java.lang.Exception e) {
            e.printStackTrace();
        }
    }
}