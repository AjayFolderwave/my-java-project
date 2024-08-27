package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class GetAccessToken {

    private static String accessToken;

    public static String fetchAccessToken() {
//        String apiUrl = "https://apipartner24.uat.commonapp.net/auth/token";
//        String userName = "24-MEFA-7bfdcf99-af61-44ea-8165-1d90093e761e";
//        String password = "c6wu9ckdhnVQnJ3cNLZB5m6SqalpyC";
//        String apiKey = "tKpciEuomX31SRdaZN0xg73rV17VgfZDkvw9wT70";

        String apiUrl = "https://apipartner24.commonapp.org/auth/token";
        String userName = "24-MEFA-bfdac51c-4e3b-4b58-be2f-05939d98c956";
        String password = "zUDf46bD6107lcNEJ5uF6gVfVTJyvQ";
        String apiKey = "KmzYqqm17d73pdemUGeSi2w0go34IqWb9RHfgEo0";

        try {
            URL url = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("x-api-key", apiKey);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);

            String jsonInputString = "{\"UserName\": \"" + userName + "\", \"Password\": \"" + password + "\"}";

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (InputStream is = connection.getInputStream();
                     BufferedReader br = new BufferedReader(new InputStreamReader(is, "utf-8"))) {

                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }

                    // Parse and extract the token from the JSON response
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode jsonNode = objectMapper.readTree(response.toString());
                    accessToken = jsonNode.get("token").asText();
                    System.out.println("Access Token: " + accessToken);
                }
            } else {
                // Handle error response
                System.out.println("Failed to get access token");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return accessToken;
    }

    public static String getAccessToken() {
        if (accessToken == null) {
            fetchAccessToken();
        }
        return accessToken;
    }
}
