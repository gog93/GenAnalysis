package com.example;

import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class GenAnalysis {
    private static SparkSession spark;


public static void doAnaliz(List<Symptom> symptoms){
    spark = SparkSession.builder()
            .appName("GeneAnalysis")
            .master("local[*]")
            .config("spark.sql.debug.maxToStringFields", "5000")
            .getOrCreate();
//    String symptomsInput = "lower_limb_amyotrophy_HP:0007210,amyotrophy_involving_the_upper_limbs_HP:0009129,hyperreflexia_in_upper_limbs_HP:0007350";
//    List<String> userSymptoms = Arrays.asList(symptomsInput.split(","));
    List<String> userSymptoms = new ArrayList<>();

    for (Symptom s:symptoms) {
        userSymptoms.add(s.getSympt());
    }

    File folder = new File("exel");
    File[] listOfFiles = folder.listFiles();

    for (File file : listOfFiles) {
        if (file.isFile() && file.getName().endsWith(".xlsx")) {
            Dataset<Row> df = loadDataFrame(file);
            Dataset<Row> filteredDF = filterData(df, userSymptoms);

            if (filteredDF.count() <= 1) {
                System.out.println("Not enough records in " + file.getName() + " to proceed with PCA.");
                continue;
            }

            processPCA(filteredDF, userSymptoms);
            calculateCorrelations(filteredDF, userSymptoms);
            calculateFrequencies(filteredDF, userSymptoms);
            filteredDF = removeRowsWithTooManyGaps(filteredDF, userSymptoms);
            filteredDF = fillMissingValuesWithMedian(filteredDF, userSymptoms);
        }
    }

    spark.stop();
}
    private static Dataset<Row> loadDataFrame(File file) {
        return spark.read().format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(file.getAbsolutePath());
    }

    private static Dataset<Row> filterData(Dataset<Row> df, List<String> userSymptoms) {
        Dataset<Row> filteredDF = df.filter("ensemble_decision = 'IN'");

        for (String symptom : userSymptoms) {
            filteredDF = filteredDF.withColumn(symptom,
                    functions.when(functions.col(symptom).equalTo("yes"), 1.0)
                            .when(functions.col(symptom).equalTo("no"), 0.0)
                            .otherwise(null));
        }

        for (String symptom : userSymptoms) {
            filteredDF = filteredDF.filter(functions.col(symptom).isNotNull());
        }

        return filteredDF;
    }

    private static void processPCA(Dataset<Row> filteredDF, List<String> userSymptoms) {
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(userSymptoms.toArray(new String[0]))
                .setOutputCol("features");
        Dataset<Row> vectorizedDF = vectorAssembler.transform(filteredDF);

        PCA pca = new PCA()
                .setInputCol("features")
                .setOutputCol("pcaFeatures")
                .setK(userSymptoms.size());
        Dataset<Row> pcaResult = pca.fit(vectorizedDF).transform(vectorizedDF);

        pcaResult.select("pcaFeatures").show(50);
    }

    private static void calculateCorrelations(Dataset<Row> filteredDF, List<String> userSymptoms) {
        for (int i = 0; i < userSymptoms.size(); i++) {
            for (int j = i + 1; j < userSymptoms.size(); j++) {
                double correlation = filteredDF.stat().corr(userSymptoms.get(i), userSymptoms.get(j));
                System.out.println("Correlation between " + userSymptoms.get(i) + " and " + userSymptoms.get(j) + ": " + correlation);
            }
        }
    }

    private static void calculateFrequencies(Dataset<Row> filteredDF, List<String> userSymptoms) {
        for (String symptom : userSymptoms) {
            Dataset<Row> symptomCounts = filteredDF.groupBy(symptom).count();
            System.out.println("Frequency of " + symptom + ":");
            symptomCounts.show();
        }
    }

    private static Dataset<Row> removeRowsWithTooManyGaps(Dataset<Row> filteredDF, List<String> userSymptoms) {
        for (String symptom : userSymptoms) {
            filteredDF = filteredDF.withColumn("symptom_missing_count", functions.when(functions.col(symptom).isNull(), 1).otherwise(0));
        }
        return filteredDF.filter(functions.col("symptom_missing_count").leq(2));
    }

    private static Dataset<Row> fillMissingValuesWithMedian(Dataset<Row> filteredDF, List<String> userSymptoms) {
        for (String symptom : userSymptoms) {
            double median = calculateMedian(filteredDF, symptom);
            filteredDF = filteredDF.withColumn(symptom,
                    functions.when(functions.col(symptom).isNull(), median).otherwise(functions.col(symptom)));
        }
        return filteredDF;
    }

    private static double calculateMedian(Dataset<Row> df, String columnName) {
        // Assuming the column values are numbers, if they are strings this will fail
        Dataset<Row> sortedDF = df.sort(columnName)
                .filter(functions.col(columnName).rlike("^[0-9]+(\\.[0-9]+)?$")); // regex to match numeric values

        // Count number of non-null values
        long count = sortedDF.count();
        if (count == 0) {
            return 0; // or some other default value
        }

        if (count % 2 == 1) {
            Row medianRow = sortedDF.limit((int) (count / 2 + 1)).collectAsList().get((int) (count / 2));
            return medianRow.getAs(columnName);
        }
        else {
            Row lowerMedianRow = sortedDF.limit((int) (count / 2)).collectAsList().get((int) (count / 2) - 1);
            Row upperMedianRow = sortedDF.limit((int) (count / 2 + 1)).collectAsList().get((int) (count / 2));
            double lowerMedian = lowerMedianRow.getAs(columnName);
            double upperMedian = upperMedianRow.getAs(columnName);
            return (lowerMedian + upperMedian) / 2.0;
        }
    }
}

