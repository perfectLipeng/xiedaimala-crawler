package com.github.hcsp;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        //待处理的连接池
        Connection connection = DriverManager.getConnection("jdbc:h2:C:\\Users\\lipeng\\IdeaProjects\\xiedaimala-crawler\\news");

        //已处理的连接池
        while (true) {
            List<String> linkPool = loadUrlsFromDB(connection, "select link from   LINKS_TO_BE_PROCESSED");
            Set<String> processedLinks = new HashSet<>(loadUrlsFromDB(connection, "select * from   LINKS_ALREADY_PROCESSED"));
            if (linkPool.isEmpty()) {
                break;
            }
            String link = linkPool.remove(linkPool.size() - 1);
            insertLinkIntoDB(connection, link, "DELETE FROM LINKS_TO_BE_PROCESSED WHERE LINK = ?");

            if (isLinkPocessed(connection, link, "select * from   LINKS_ALREADY_PROCESSED where (link) = ?")) {
                continue;
            }
            if (isIntresting(link)) {
                Document doc = httpGetAndParseHtml(link);
                paseUrlsFromPageAndStoreIntoDb(connection, doc);
                storeIntoDbIfItIsNewsPage(doc);
                insertLinkIntoDB(connection, link, "INSERT INTO LINKS_ALREADY_PROCESSED (link) values (?)");
            }

        }


    }

    private static void paseUrlsFromPageAndStoreIntoDb(Connection connection, Document doc) throws SQLException {
        for (Element aTag : doc.select("a")) {
            String href = aTag.attr("href");
            insertLinkIntoDB(connection, href, "INSERT INTO LINKS_TO_BE_PROCESSED (link) values (?)");
        }
    }

    private static boolean isLinkPocessed(Connection connection, String link, String qrySql) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(qrySql)) {
            preparedStatement.setString(1, link);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return true;
            }
        }
        return false;
    }

    private static void insertLinkIntoDB(Connection connection, String href, String addSql) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(addSql)) {
            preparedStatement.setString(1, href);
            preparedStatement.executeUpdate();
        }
    }

    private static List<String> loadUrlsFromDB(Connection connection, String sql) throws SQLException {
        List<String> results = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                results.add(resultSet.getString(1));
            }
        }
        return results;
    }

    private static void storeIntoDbIfItIsNewsPage(Document doc) {
        Elements articleElement = doc.select("article");
        if (!articleElement.isEmpty()) {
            for (Element articleTag : articleElement) {
                String title = articleTag.child(0).text();
            }
        }
    }

    public static Document httpGetAndParseHtml(String link) throws IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        if (link.startsWith("//")) {
            link += "https:";
        }
        HttpGet httpGet = new HttpGet(link);
        httpGet.addHeader("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36");
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            HttpEntity responseEntity = response.getEntity();
            System.out.println("响应状态为:" + response.getStatusLine());
            String html = EntityUtils.toString(responseEntity, "UTF-8");
            return Jsoup.parse(html);
        }
    }

    public static boolean isIntresting(String link) {
        return isNewsPage(link) || isIndexPage(link) && isNotLogin(link);
    }

    public static boolean isNewsPage(String link) {
        return link.contains("news.sina.cn");
    }

    public static boolean isIndexPage(String link) {
        return "https://sina.cn".equals(link);
    }

    public static boolean isNotLogin(String link) {
        return !link.contains("passport.sina.cn");
    }
}