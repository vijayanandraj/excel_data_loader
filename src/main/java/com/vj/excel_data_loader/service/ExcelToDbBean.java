package com.vj.excel_data_loader.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

@Component
@Slf4j
public class ExcelToDbBean {

    private static final int BATCH_SIZE = 1000;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public void processExcelToDb(String excelFilePath) throws IOException {
        FileInputStream fis = new FileInputStream(excelFilePath);
        Workbook workbook = new XSSFWorkbook(fis);
        Sheet sheet = workbook.getSheetAt(0);
        Iterator<Row> rowIterator = sheet.iterator();

        Map<Integer, Integer> columnSizes = new HashMap<>();
        StringJoiner sqlColumns = new StringJoiner(", ");

        // Read first row as headers and initialize column sizes
        Row headerRow = rowIterator.next();
        Iterator<Cell> headerCellIterator = headerRow.cellIterator();
        int columnIndex = 0;
        while (headerCellIterator.hasNext()) {
            Cell cell = headerCellIterator.next();
            columnSizes.put(columnIndex, cell.getStringCellValue().length());
            columnIndex++;
        }

        // Determine max column sizes
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Iterator<Cell> cellIterator = row.cellIterator();

            columnIndex = 0;
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                int cellLength = cell.toString().length();
                int currentMaxSize = columnSizes.getOrDefault(columnIndex, 0);
                if (cellLength > currentMaxSize) {
                    int adjustedSize = (int) Math.ceil((double) cellLength / 10) * 10;
                    columnSizes.put(columnIndex, adjustedSize);
                }
                columnIndex++;
            }
        }

        // Reset row iterator
        rowIterator = sheet.iterator();
        rowIterator.next(); // Skip the header row

        // Generate SQL for table creation
        headerCellIterator = headerRow.cellIterator();
        columnIndex = 0;
        while (headerCellIterator.hasNext()) {
            Cell cell = headerCellIterator.next();
            String headerName = normalizeColumnName(cell.getStringCellValue());
            sqlColumns.add(headerName + " VARCHAR(" + columnSizes.get(columnIndex) + ")");
            columnIndex++;
        }

        // Create table
        String fileName = new File(excelFilePath).getName();
        int pos = fileName.lastIndexOf(".");
        if (pos > 0) {
            fileName = fileName.substring(0, pos);
        }
        String tableName = fileName;
        String createTableSQL = "";
        if (doesTableExist(tableName)) {
            String truncateTableSQL = "TRUNCATE TABLE " + tableName;
            jdbcTemplate.execute(truncateTableSQL);
        } else {
            createTableSQL = "CREATE TABLE " + tableName + " (" + sqlColumns.toString() + ")";
            jdbcTemplate.execute(createTableSQL);
        }

//        String createTableSQL = "CREATE TABLE " + tableName + " (" + sqlColumns.toString() + ")";
//        log.info("Table name ==> {}",createTableSQL);
//        jdbcTemplate.execute(createTableSQL);

        // Insert data in batches
        int batchCount = 0;
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            StringJoiner columnValues = new StringJoiner(", ");
            Iterator<Cell> cellIterator = row.cellIterator();

            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                Object value;
                if (cell.getCellType() == CellType.NUMERIC) {
                    double numericValue = cell.getNumericCellValue();
                    if (numericValue == Math.floor(numericValue)) {
                        value = (int) numericValue;
                    } else {
                        value = numericValue;
                    }
                } else {
                    value = cell.toString();
                }
                columnValues.add("'" + value + "'");
            }

            String insertSQL = "INSERT INTO " + tableName + " VALUES (" + columnValues.toString() + ")";
            jdbcTemplate.update(insertSQL);

            if (++batchCount % BATCH_SIZE == 0) {
                // In a real-world scenario, you'd batch the SQL statements here
            }
        }

        // Output DDL
        log.info("Table created with SQL: {}",createTableSQL);
    }

    private String normalizeColumnName(String columnName) {
        return columnName.replaceAll("\\s+", "_").replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
    }

    private boolean doesTableExist(String tableName) {
        String sql = "SHOW TABLES LIKE ?";
        List<String> result = jdbcTemplate.queryForList(sql, new Object[]{tableName}, String.class);
        return !result.isEmpty();
    }

}
