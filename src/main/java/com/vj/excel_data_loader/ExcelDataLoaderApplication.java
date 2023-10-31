package com.vj.excel_data_loader;

import com.vj.excel_data_loader.service.ExcelToDbBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ExcelDataLoaderApplication implements CommandLineRunner {

    @Autowired
    private ExcelToDbBean excelToDbBean;

    public static void main(String[] args) {
        SpringApplication.run(ExcelDataLoaderApplication.class, args);
    }


    @Override
    public void run(String... args) throws Exception {
        excelToDbBean.processExcelToDb("E:/docs/ex1.xlsx");
    }
}
