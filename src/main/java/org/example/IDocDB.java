package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

public class IDocDB {

    private static Properties properties = new Properties();
    private static String jdbcUrl;
    private static String insertSql;
    private static String teamsWebhookUrl;
    private static String sqlQuery1;
    private static String sqlQuery2;

    static {
        try (InputStream input = new FileInputStream("src/main/java/org/example/application.properties")) {
            properties.load(input);
            jdbcUrl = properties.getProperty("jdbc.url");
            insertSql = properties.getProperty("jdbc.insert_sql");
            teamsWebhookUrl = properties.getProperty("teams.webhook_url");
            sqlQuery1 = properties.getProperty("sql.query1");
            sqlQuery2 = properties.getProperty("sql.query2");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting SendGrid API process...");
        String exportJobId = createExportJob();
        if (exportJobId != null) {
            System.out.println("Export job created with ID: " + exportJobId);
            fetchExportedContacts(exportJobId);
            String queryResults = executeSQLQuery();
            sendTeamsMessage(queryResults);
        } else {
            System.out.println("Failed to create export job.");
        }
    }

    private static String createExportJob() {
        String endpoint = "https://api.sendgrid.com/v3/marketing/contacts/exports";
        HttpClient client = HttpClient.newHttpClient();
//        String requestBody = "{ \"list_ids\": [\"ca183608-6ed7-44a8-a9c9-cc95227fa4ee\"] }";
        String requestBody = "{ \"list_ids\": [\"887069a4-8aac-497a-a804-808b99096127\"] }";




        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .header("Authorization", "Bearer " + System.getenv("SENDGRID_API_KEY"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 202) {
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode responseBody = objectMapper.readTree(response.body());
                System.out.println("Export job creation response: " + response.body());
                return responseBody.path("id").asText();
            } else {
                System.out.println("Error creating export job: " + response.statusCode());
                System.out.println(response.body());
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void fetchExportedContacts(String exportJobId) {
        String endpoint = "https://api.sendgrid.com/v3/marketing/contacts/exports/" + exportJobId;
        HttpClient client = HttpClient.newHttpClient();

        while (true) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Authorization", "Bearer " + System.getenv("SENDGRID_API_KEY"))
                    .header("Content-Type", "application/json")
                    .GET()
                    .build();

            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode responseBody = objectMapper.readTree(response.body());

                    String status = responseBody.path("status").asText();
                    System.out.println("Export job status: " + status);
                    if ("ready".equals(status)) {
                        System.out.println("Exported contacts fetched successfully.");
                        System.out.println("Response Body: " + response.body());

                        String downloadUrl = responseBody.path("urls").get(0).asText();
                        System.out.println("Download URL: " + downloadUrl);
                        downloadAndProcessCSV(downloadUrl);
                        break;
                    } else {
                        Thread.sleep(5000); // Wait for 5 seconds before checking again
                    }
                } else {
                    System.out.println("Error fetching exported contacts: " + response.statusCode());
                    System.out.println(response.body());
                    break;
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    private static void downloadAndProcessCSV(String url) {
        try {
            URL downloadUrl = new URL(url);
            String outputFileName = "exported_contacts.csv.gzip";

            // Download the file
            try (ReadableByteChannel readableByteChannel = Channels.newChannel(downloadUrl.openStream());
                 FileOutputStream fileOutputStream = new FileOutputStream(outputFileName)) {
                fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            }

            // Unzip the file
            String csvFileName = "exported_contacts.csv";
            try (GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(outputFileName));
                 FileOutputStream out = new FileOutputStream(csvFileName)) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = gzipInputStream.read(buffer)) > 0) {
                    out.write(buffer, 0, len);
                }
            }

            // Process the CSV file
            processCSV(csvFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processCSV(String csvFileName) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFileName)));
             Connection connection = DriverManager.getConnection(jdbcUrl);
             PreparedStatement statement = connection.prepareStatement(insertSql)) {

            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);

            for (CSVRecord record : records) {
                statement.setString(1, record.get("EMAIL"));
                statement.setString(2, record.get("FIRST_NAME"));
                statement.setString(3, record.get("LAST_NAME"));
                statement.setString(4, record.get("ADDRESS_LINE_1"));
                statement.setString(5, record.get("ADDRESS_LINE_2"));
                statement.setString(6, record.get("CITY"));
                statement.setString(7, record.get("STATE_PROVINCE_REGION"));
                statement.setString(8, record.get("POSTAL_CODE"));
                statement.setString(9, record.get("COUNTRY"));
                statement.setString(10, record.isMapped("ALTERNATE_EMAILS") ? record.get("ALTERNATE_EMAILS") : null);
                statement.setString(11, record.isMapped("PHONE_NUMBER") ? record.get("PHONE_NUMBER") : null);
                statement.setString(12, record.isMapped("WHATSAPP") ? record.get("WHATSAPP") : null);
                statement.setString(13, record.isMapped("LINE") ? record.get("LINE") : null);
                statement.setString(14, record.isMapped("FACEBOOK") ? record.get("FACEBOOK") : null);
                statement.setString(15, record.isMapped("UNIQUE_NAME") ? record.get("UNIQUE_NAME") : null);
                statement.setString(16, record.isMapped("CREATED_AT") ? parseDate(record.get("CREATED_AT"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : null);
                statement.setString(17, record.isMapped("UPDATED_AT") ? parseDate(record.get("UPDATED_AT"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : null);
                statement.setString(18, record.isMapped("CONTACT_ID") ? record.get("CONTACT_ID") : null);
                statement.setString(19, record.isMapped("IDOCEnabled") ? record.get("IDOCEnabled") : null);
                statement.setString(20, record.isMapped("DocumentReceived") ? record.get("DocumentReceived") : null);
                statement.setString(21, record.isMapped("CBFinAidID") ? record.get("CBFinAidID") : null);
                statement.setString(22, record.isMapped("AwardYear") ? record.get("AwardYear") : null);
                statement.setString(23, record.isMapped("PreferredName") ? record.get("PreferredName") : null);
                statement.setString(24, record.isMapped("ProfileFileAvailable") ? record.get("ProfileFileAvailable") : null);
                statement.setString(25, record.isMapped("ProfileSubmitDate") ? parseDate(record.get("ProfileSubmitDate"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : null);
                statement.setString(26, record.isMapped("CSSID") ? record.get("CSSID") : null);
                statement.setString(27, record.isMapped("OrganizationName") ? record.get("OrganizationName") : null);
                statement.setString(28, record.isMapped("AwardYearText") ? record.get("AwardYearText") : null);

                statement.addBatch();
            }
            int[] result = statement.executeBatch();
            System.out.println("Inserted rows: " + result.length);
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static String parseDate(String date, DateTimeFormatter sqlFormatter) {
        if (date == null || date.isEmpty()) {
            return null;
        }

        // Normalize the date string to a consistent format
        String normalizedDate = date.replaceAll("Z$", "");
        try {
            if (normalizedDate.contains(".")) {
                normalizedDate = normalizedDate.replaceAll("\\.\\d{3,}", "");
            }
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
            LocalDateTime dateTime = LocalDateTime.parse(normalizedDate, formatter);
            return dateTime.format(sqlFormatter);
        } catch (DateTimeParseException e) {
            System.err.println("Failed to parse date: " + date);
            e.printStackTrace();
            return null;
        }
    }
    private static String executeSQLQuery() {
        StringBuilder results = new StringBuilder();

        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement()) {
            System.out.println("Executing first SQL query...");
            statement.execute(sqlQuery1);

            System.out.println("Executing second SQL query...");
            boolean hasResults = statement.execute(sqlQuery2);

            while (hasResults || statement.getUpdateCount() != -1) {
                if (hasResults) {
                    try (ResultSet resultSet = statement.getResultSet()) {
                        while (resultSet.next()) {
                            String totalMissingDocRcvdDt = resultSet.getString("TOTAL_MISSING_DOCRCVD_DT");
                            String atRisk = resultSet.getString("AT_RISK");
                            String todayDocRcvd = resultSet.getString("TODAY_DOC_RECVD");
                            String todayAtRisk = resultSet.getString("TODAY_AT_RISK");
                            String minDaysFrom2024 = resultSet.getString("MIN_DAYS_FROM_2024");
                            String nullEmails = resultSet.getString("NULL_EMAILS");

                            results.append("Total Missing Document Received Date: ").append(totalMissingDocRcvdDt).append(",\n")
                                    .append("At Risk: ").append(atRisk).append(",\n")
                                    .append("Today Document Received: ").append(todayDocRcvd).append(",\n")
                                    .append("Today At Risk: ").append(todayAtRisk).append(",\n")
                                    .append("Min Days From 2024-01-01: ").append(minDaysFrom2024).append(",\n")
                                    .append("Null Emails: ").append(nullEmails).append("\n\n");
                        }
                    }
                }
                hasResults = statement.getMoreResults();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return results.toString();
    }

    private static void sendTeamsMessage(String message) {
        HttpClient client = HttpClient.newHttpClient();
        String requestBody = String.format("{\"text\": \"%s\"}", message.replace("\n", "\\n").replace("\"", "\\\""));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(teamsWebhookUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                System.out.println("Message sent to Teams group successfully.");
            } else {
                System.out.println("Failed to send message to Teams group: " + response.statusCode());
                System.out.println(response.body());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
