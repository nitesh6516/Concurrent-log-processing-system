import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class LogEntry {
    private String userId;
    private String action;

    public LogEntry(String userId, String action) {
        this.userId = userId;
        this.action = action;
    }

    public String getUserId() {
        return userId;
    }

    public String getAction() {
        return action;
    }
}

class ActivityAggregator {
    private ConcurrentHashMap<String, Integer> userActivityCount = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> actionTypeCount = new ConcurrentHashMap<>();

    public void recordActivity(String userId, String action) {
        userActivityCount.merge(userId, 1, Integer::sum);
        actionTypeCount.merge(action, 1, Integer::sum);
    }

    public Map<String, Integer> getUserActivityCount() {
        return userActivityCount;
    }

    public Map<String, Integer> getActionTypeCount() {
        return actionTypeCount;
    }
}

class LogProcessor implements Runnable {
    private File logFile;
    private ActivityAggregator aggregator;

    public LogProcessor(File logFile, ActivityAggregator aggregator) {
        this.logFile = logFile;
        this.aggregator = aggregator;
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                LogEntry entry = parseLine(line);
                if (entry != null) {
                    aggregator.recordActivity(entry.getUserId(), entry.getAction());
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + logFile.getName() + " - " + e.getMessage());
        }
    }

    private LogEntry parseLine(String line) {
        String[] parts = line.split("\\s+");
        if (parts.length != 3) {
            return null;
        }

        String timestamp = parts[0];
        String userId = parts[1];
        String action = parts[2];

        if (!timestamp.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}")) {
            return null;
        }

        if (userId.isEmpty() || action.isEmpty()) {
            return null;
        }

        return new LogEntry(userId, action);
    }
}

class ReportGenerator {
    public void generateReport(ActivityAggregator aggregator, String outputPath) {
        Map<String, Integer> userCounts = aggregator.getUserActivityCount();

        List<Map.Entry<String, Integer>> sortedUsers = userCounts.entrySet()
                .stream()
                .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                .collect(Collectors.toList());

        try (PrintWriter writer = new PrintWriter(outputPath)) {
            writer.println("===== USER ACTIVITY REPORT =====");

            for (Map.Entry<String, Integer> entry : sortedUsers) {
                writer.println(entry.getKey() + ": " + entry.getValue() + " actions");
            }

            if (!sortedUsers.isEmpty()) {
                writer.println();
                writer.println("Most active user: " + sortedUsers.get(0).getKey()
                        + " (" + sortedUsers.get(0).getValue() + " actions)");
            }

            Map<String, Integer> actionCounts = aggregator.getActionTypeCount();
            if (!actionCounts.isEmpty()) {
                writer.println();
                writer.println("----- Actions Breakdown -----");
                for (Map.Entry<String, Integer> entry : actionCounts.entrySet()) {
                    writer.println(entry.getKey() + ": " + entry.getValue());
                }
            }

        } catch (IOException e) {
            System.err.println("Failed to write report: " + e.getMessage());
        }
    }
}

public class ConcurrentLogProcessingSystem {
    public static void main(String[] args) {
        String logDirectory = "logs";
        String outputFile = "report.txt";

        File dir = new File(logDirectory);

        if (!dir.exists() || !dir.isDirectory()) {
            System.out.println("Directory not found: " + logDirectory);
            return;
        }

        File[] files = dir.listFiles((d, name) -> name.endsWith(".txt"));

        if (files == null || files.length == 0) {
            System.out.println("No log files found in directory.");
            return;
        }

        ActivityAggregator aggregator = new ActivityAggregator();
        Thread[] threads = new Thread[files.length];

        for (int i = 0; i < files.length; i++) {
            LogProcessor processor = new LogProcessor(files[i], aggregator);
            threads[i] = new Thread(processor);
            threads[i].start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                System.err.println("Thread interrupted: " + e.getMessage());
            }
        }

        ReportGenerator reportGenerator = new ReportGenerator();
        reportGenerator.generateReport(aggregator, outputFile);

        System.out.println("Report generated successfully: " + outputFile);
    }
}
