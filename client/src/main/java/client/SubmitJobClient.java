package client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import com.google.gson.Gson;

public class SubmitJobClient {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Uso: SubmitJobClient <filePath> <jobId> <numChunks> <outputPath>");
            System.exit(1);
        }

        String rawFilePath = args[0];
        int jobId = Integer.parseInt(args[1]);
        int totalChunks = Integer.parseInt(args[2]);
        String outputPath = args[3];

        File fileToProcess = new File(rawFilePath);
        
        if (!fileToProcess.exists()) {
            File parentFile = new File("../" + rawFilePath);
            if (parentFile.exists()) {
                fileToProcess = parentFile;
                System.out.println("[Client] Archivo encontrado en la raíz del proyecto.");
            } else {
                System.err.println("[Client] ERROR CRÍTICO: El archivo no existe en: " + fileToProcess.getAbsolutePath());
                System.err.println("         Ni tampoco en: " + parentFile.getAbsolutePath());
                System.exit(1);
            }
        }

        try {
            Manifest manifest = new Manifest();
            manifest.jobId = jobId;
            manifest.totalChunks = totalChunks;
            manifest.outputPath = outputPath;
            manifest.filePath = fileToProcess.getCanonicalPath(); 

            File targetDir = new File("../master/manifests");
            if (!targetDir.exists()) {
                 targetDir = new File("master/manifests");
            }
            if (!targetDir.exists()) {
                targetDir.mkdirs();
            }

            String jsonFileName = new File(targetDir, "job_" + jobId + ".json").getAbsolutePath();
            Gson gson = new Gson();

            try (FileWriter writer = new FileWriter(jsonFileName)) {
                gson.toJson(manifest, writer);
                System.out.println("[Client] ¡ÉXITO! JSON escrito en: " + jsonFileName);
                System.out.println("[Client] Ruta enviada al Master: " + manifest.filePath);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class Manifest {
        int jobId;
        int totalChunks;
        String outputPath;
        String filePath;
    }
}