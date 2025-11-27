package worker;

import SITM.*;
import com.zeroc.Ice.Current;
import com.zeroc.Ice.Communicator;
import java.io.RandomAccessFile;
import java.util.*;

public class WorkerImpl implements SITM.Worker {
    private Communicator communicator;

    public WorkerImpl(Communicator communicator) {
        this.communicator = communicator;
    }

    @Override
    public void processTask(int jobId, int chunkId, String fileName, long startOffset, long dataSize, MasterPrx master, Current current) {
        System.out.println("[Worker] Procesando Chunk " + chunkId + " del Job " + jobId);
        
        Map<Integer, double[]> localStats = new HashMap<>(); 
        Map<Integer, double[]> busLastPosition = new HashMap<>();
        int validLines = 0;
        int errorLines = 0;

        try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
            file.seek(startOffset);
            long endPosition = startOffset + dataSize;
            
            if (startOffset > 0) {
                file.readLine();
            }

            String line;
            while (file.getFilePointer() < endPosition && (line = file.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                
                if (line.startsWith("datagramId") || line.startsWith("routeId")) continue;

                String[] parts = line.split(",");
                if (parts.length < 6) continue; 

                try {
                    int busId = Integer.parseInt(parts[1].trim());
                    double lat = Double.parseDouble(parts[2].trim());
                    double lon = Double.parseDouble(parts[3].trim());
                    long time = Long.parseLong(parts[4].trim());
                    int routeId = Integer.parseInt(parts[5].trim());

                    if (busLastPosition.containsKey(busId)) {
                        double[] last = busLastPosition.get(busId);
                        double distKm = haversine(last[0], last[1], lat, lon);
                        double timeDiffHours = Math.abs(time - last[2]) / 3600.0; 

                        if (timeDiffHours > 0) {
                            double speed = distKm / timeDiffHours;

                            if (speed < 150 && speed >= 0) { // Filtro un poco más relajado
                                localStats.putIfAbsent(routeId, new double[]{0.0, 0.0});
                                double[] s = localStats.get(routeId);
                                s[0] += speed;
                                s[1] += 1.0;
                                validLines++;
                            }
                        }
                    }
                    busLastPosition.put(busId, new double[]{lat, lon, (double) time});

                } catch (Exception e) { 
                    errorLines++;
                }
            }

            List<ArcStat> results = new ArrayList<>();
            for (Map.Entry<Integer, double[]> entry : localStats.entrySet()) {
                ArcStat stat = new ArcStat();
                stat.routeId = entry.getKey();
                stat.sumSpeed = entry.getValue()[0];
                stat.count = (int) entry.getValue()[1];
                results.add(stat);
            }

            if (master == null) {
                 String masterStr = communicator.getProperties().getProperty("Master.Proxy");
                 master = MasterPrx.checkedCast(communicator.stringToProxy(masterStr));
            }

            System.out.println("[Worker] Chunk " + chunkId + ": Líneas válidas=" + validLines + ", Errores/Header=" + errorLines);
            System.out.println("[Worker] Reportando " + results.size() + " rutas.");
            
            master.reportResult(jobId, chunkId, results.toArray(new ArcStat[0]));

        } catch (Exception e) {
            System.err.println("[Worker] Error crítico: " + e.getMessage());
            e.printStackTrace();
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