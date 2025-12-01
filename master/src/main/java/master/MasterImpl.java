package master;

import SITM.*;
import com.zeroc.Ice.Current;
import com.zeroc.Ice.Communicator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;
import java.io.*;
import java.nio.file.*;
import com.google.gson.Gson;
import java.util.Map;
import java.util.HashMap;


public class MasterImpl implements SITM.Master {

    private final ConcurrentMap<Integer, JobState> jobs = new ConcurrentHashMap<>();
    private final List<WorkerPrx> workers = Collections.synchronizedList(new ArrayList<>());

    private final ConcurrentHashMap<Integer, Double> liveSpeedMap = new ConcurrentHashMap<>();
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    private final Communicator communicator;
    private final Path manifestDir = Paths.get("master/manifests");
    private final Gson gson = new Gson();
    private final ExecutorService executor;

    // ---------------- OBSERVER ----------------
    private final List<TrafficSubscriberPrx> subscribers =
            Collections.synchronizedList(new ArrayList<>());

    // ------------------------------------------

    public MasterImpl(Communicator communicator) {
        this.communicator = communicator;
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public void registerWorker(WorkerPrx worker, Current current) {
        if (!workers.contains(worker)) {
            workers.add(worker);
            System.out.println("[Master] Nuevo Worker registrado. Total activos: " + workers.size());
        }
    }

    @Override
    public void reportResult(int jobId, int chunkId, SITM.ArcStat[] stats, Current current) {

        // STREAMING en tiempo real
        if (jobId == -1) {
            for (ArcStat stat : stats) {
                liveSpeedMap.compute(stat.routeId, (k, v) -> {
                    double currentAvg = (stat.count > 0) ? (stat.sumSpeed / stat.count) : 0;

                    if (v == null) return currentAvg;
                    return (v + currentAvg) / 2.0;
                });
            }

            // 🔔 Notificar a los suscriptores
            notifySubscribers();
            return;
        }

        // --------- JOB NORMAL ----------
        JobState js = jobs.get(jobId);
        if (js == null) {
            System.out.println("[Master] Error: JobId desconocido: " + jobId);
            return;
        }

        js.addPartial(chunkId, stats);
        System.out.println("[Master] Job " + jobId + " | Chunk " + chunkId + " procesado.");

        synchronized(js) {
            if (js.isComplete() && !js.isFinished()) {
                js.markFinished();
                js.aggregateAndPersist();

                long elapsedMs = (System.nanoTime() - js.startTime) / 1_000_000;
                System.out.println("--------------------------------------------------");
                System.out.println("[Master] ¡JOB " + jobId + " COMPLETADO EXITOSAMENTE!");
                System.out.println("[Master] Tiempo total: " + elapsedMs + " ms");
                System.out.println("--------------------------------------------------");
            }
        }
    }

    @Override
    public void ingestStream(String[] lines, Current current) {
        if (workers.isEmpty()) return;

        int idx = roundRobinIndex.getAndIncrement() % workers.size();
        if (idx < 0) idx = 0;

        WorkerPrx worker = workers.get(idx);

        executor.submit(() -> {
            try {
                worker.processStream(lines, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void jobFinished(int jobId, Current current) { }

    public void submitJobFromClient(int jobId, int totalChunks, String outputPath, String filePath) throws InterruptedException {
        while (workers.isEmpty()) {
            System.out.println("[Master] Esperando al menos un Worker...");
            Thread.sleep(1000);
        }

        if (workers.isEmpty()) {
            System.out.println("[Master] ERROR: No hay workers registrados.");
            return;
        }

        JobState js = new JobState(jobId, totalChunks, outputPath);
        js.setStartTime(System.nanoTime());
        jobs.put(jobId, js);
        System.out.println("[Master] Job " + jobId + " iniciado. Archivo: " + filePath);

        File inputFile = new File(filePath);
        if (!inputFile.exists()) {
            System.out.println("[Master] ERROR: Archivo no existe: " + filePath);
            return;
        }

        long fileSize = inputFile.length();
        long chunkSize = fileSize / totalChunks;

        for (int i = 0; i < totalChunks; i++) {
            long startOffset = i * chunkSize;
            long size = (i == totalChunks - 1) ? (fileSize - startOffset) : chunkSize;

            WorkerPrx assignedWorker = workers.get(i % workers.size());
            final int chunkId = i;

            executor.submit(() -> {
                try {
                    assignedWorker.processTask(jobId, chunkId, filePath, startOffset, size, null);
                } catch (Exception e) {
                    System.err.println("[Master] Falló envío a worker: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }
    }

    // ----------------- OBSERVER API -----------------

    @Override
    public void registerTrafficSubscriber(TrafficSubscriberPrx sub, Current c) {
        subscribers.add(sub);
        System.out.println("[Observer] Nuevo suscriptor agregado: " + sub);
    }

    @Override
    public void unregisterTrafficSubscriber(TrafficSubscriberPrx sub, Current c) {
        subscribers.remove(sub);
        System.out.println("[Observer] Suscriptor eliminado.");
    }

    private void notifySubscribers() {
        if (subscribers.isEmpty()) return;

        Map<Integer, Double> snapshot = new HashMap<>();
        snapshot.putAll(liveSpeedMap);

        synchronized (subscribers) {
            subscribers.removeIf(sub -> {
                try {
                    sub.updateSpeeds(snapshot);
                    return false;
                } catch (Exception e) {
                    System.out.println("[Observer] Suscriptor desconectado, se elimina.");
                    return true;
                }
            });
        }
    }

    // ----------------- WATCHERS Y MONITOR -----------------

    public void startManifestWatcher() {
        new Thread(() -> {
            System.out.println("[LiveMonitor] Tablero de tiempo real activo.");
            while (true) {
                try {
                    Thread.sleep(3000);
                    if (!liveSpeedMap.isEmpty()) {
                        System.out.println("\n--- 🔴 TRÁFICO EN TIEMPO REAL ---");
                        liveSpeedMap.forEach((route, speed) -> {
                            System.out.printf("Ruta %d: %.2f km/h\n", route, speed);
                        });
                        System.out.println("---------------------------------");
                    }
                } catch (InterruptedException e) {}
            }
        }).start();

        new Thread(() -> {
            if (!Files.exists(manifestDir)) {
                try { Files.createDirectories(manifestDir); } catch (IOException e) { e.printStackTrace(); }
            }
            System.out.println("[Master] Monitor de carpetas activo (Polling mode): " + manifestDir.toAbsolutePath());

            while (true) {
                try {
                    File folder = manifestDir.toFile();
                    File[] files = folder.listFiles((dir, name) -> name.endsWith(".json") && !name.endsWith(".done"));

                    if (files != null) {
                        for (File file : files) {
                            System.out.println("[Master] Archivo detectado: " + file.getName());

                            String content = new String(Files.readAllBytes(file.toPath()));
                            Manifest manifest = gson.fromJson(content, Manifest.class);

                            if (manifest.filePath != null) {
                                System.out.println("[Master] Procesando Job " + manifest.jobId + "...");
                                submitJobFromClient(manifest.jobId, manifest.totalChunks, manifest.outputPath, manifest.filePath);

                                File doneFile = new File(folder, file.getName() + ".done");
                                file.renameTo(doneFile);
                            }
                        }
                    }
                    Thread.sleep(2000);

                } catch (Exception e) {
                    System.err.println("[Master] Error en el watcher: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }).start();
    }

    static class Manifest {
        int jobId;
        int totalChunks;
        String outputPath;
        String filePath;
    }
}
