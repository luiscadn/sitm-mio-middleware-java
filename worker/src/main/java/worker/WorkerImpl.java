package worker;

import SITM.*;
import com.zeroc.Ice.Current;
import com.zeroc.Ice.Communicator;
import java.io.RandomAccessFile;
import java.util.*;
import java.text.SimpleDateFormat;

public class WorkerImpl implements SITM.Worker {
    private Communicator communicator;
    
    private static final Map<Integer, double[]> busLastPosition = new HashMap<>();

    public WorkerImpl(Communicator communicator) {
        this.communicator = communicator;
    }

    @Override
    public void processStream(String[] lines, MasterPrx master, Current current) {
        Map<Integer, double[]> localStats = new HashMap<>(); 
        int processedCount = 0;

        for (String line : lines) {
            if (line.trim().isEmpty()) continue; 

            String[] parts = line.split(",");
            if (parts.length < 11) continue;

            try {
                int busId = Integer.parseInt(parts[2].trim());
                
                double lat = Double.parseDouble(parts[4].trim()) / 10000000.0;
                double lon = Double.parseDouble(parts[5].trim()) / 10000000.0;
                
                long time = 0;
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    time = sdf.parse(parts[10].trim()).getTime() / 1000;
                } catch(Exception ex) {
                    continue; 
                }

                int routeId = Integer.parseInt(parts[11].trim());

                synchronized(busLastPosition) {
                    if (busLastPosition.containsKey(busId)) {
                        double[] last = busLastPosition.get(busId);
                        double distKm = haversine(last[0], last[1], lat, lon);
                        double timeDiffHours = Math.abs(time - last[2]) / 3600.0; 

                        if (timeDiffHours > 0) {
                            double speed = distKm / timeDiffHours;
                            if (speed > 0 && speed < 150) { 
                                localStats.putIfAbsent(routeId, new double[]{0.0, 0.0});
                                double[] s = localStats.get(routeId);
                                s[0] += speed;
                                s[1] += 1.0;
                                processedCount++;
                            }
                        }
                    }
                    busLastPosition.put(busId, new double[]{lat, lon, (double) time});
                }
            } catch (Exception e) { 
            }
        }

        if (processedCount > 0) {
            System.out.println("[Worker] Stream Batch: Calculadas " + processedCount + " velocidades.");
        }

        List<ArcStat> results = new ArrayList<>();
        for (Map.Entry<Integer, double[]> entry : localStats.entrySet()) {
            ArcStat stat = new ArcStat();
            stat.routeId = entry.getKey();
            stat.sumSpeed = entry.getValue()[0];
            stat.count = (int) entry.getValue()[1];
            results.add(stat);
        }

        if (!results.isEmpty()) {
            if (master == null) {
                 String masterStr = communicator.getProperties().getProperty("Master.Proxy");
                 master = MasterPrx.uncheckedCast(communicator.stringToProxy(masterStr));
            }
            master.reportResult(-1, 0, results.toArray(new ArcStat[0]));
        }
    }
    
    @Override
    public void processTask(int jobId, int chunkId, String fileName, long startOffset, long dataSize, MasterPrx master, Current current) {
        System.out.println("[Worker] Procesando Job " + jobId + " Chunk " + chunkId);
        Map<Integer, double[]> localStats = new HashMap<>();
        int processedCount = 0;

        try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
            file.seek(startOffset);
            long endPosition = startOffset + dataSize;

            // Si no es el primer chunk, descartamos la primera línea por si está incompleta
            if (startOffset > 0) {
                file.readLine();
            }

            String line;
            while (file.getFilePointer() < endPosition && (line = file.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                String[] parts = line.split(",");
                if (parts.length < 12) continue; // Asegurarse que la línea tiene todas las partes

                try {
                    int busId = Integer.parseInt(parts[2].trim());
                    double lat = Double.parseDouble(parts[4].trim()) / 10000000.0;
                    double lon = Double.parseDouble(parts[5].trim()) / 10000000.0;

                    long time = 0;
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        time = sdf.parse(parts[10].trim()).getTime() / 1000;
                    } catch (Exception ex) {
                        continue;
                    }

                    int routeId = Integer.parseInt(parts[11].trim());

                    synchronized (busLastPosition) {
                        if (busLastPosition.containsKey(busId)) {
                            double[] last = busLastPosition.get(busId);
                            double distKm = haversine(last[0], last[1], lat, lon);
                            double timeDiffHours = Math.abs(time - last[2]) / 3600.0;

                            if (timeDiffHours > 0.0001) { // Evitar división por cero
                                double speed = distKm / timeDiffHours;
                                if (speed > 0 && speed < 150) {
                                    localStats.putIfAbsent(routeId, new double[]{0.0, 0.0});
                                    double[] s = localStats.get(routeId);
                                    s[0] += speed;
                                    s[1] += 1.0;
                                    processedCount++;
                                }
                            }
                        }
                        busLastPosition.put(busId, new double[]{lat, lon, (double) time});
                    }
                } catch (Exception e) {
                    // Ignorar líneas con mal formato
                }
            }
        } catch (Exception e) {
            System.err.println("[Worker] Error procesando chunk " + chunkId + ": " + e.getMessage());
            e.printStackTrace();
        }

        if (processedCount > 0) {
            System.out.println("[Worker] Job " + jobId + " Chunk " + chunkId + ": Calculadas " + processedCount + " velocidades.");
        }

        List<ArcStat> results = new ArrayList<>();
        for (Map.Entry<Integer, double[]> entry : localStats.entrySet()) {
            ArcStat stat = new ArcStat();
            stat.routeId = entry.getKey();
            stat.sumSpeed = entry.getValue()[0];
            stat.count = (int) entry.getValue()[1];
            results.add(stat);
        }

        // Es CRUCIAL reportar siempre el resultado, incluso si está vacío,
        // para que el Master sepa que el chunk ha terminado.
        try {
            if (master == null) {
                String masterStr = communicator.getProperties().getProperty("Master.Proxy");
                master = MasterPrx.uncheckedCast(communicator.stringToProxy(masterStr));
            }
            master.reportResult(jobId, chunkId, results.toArray(new ArcStat[0]));
        } catch (Exception e) {
            System.err.println("[Worker] Error al reportar resultados del chunk " + chunkId + ": " + e.getMessage());
        }
    }

    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; 
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}