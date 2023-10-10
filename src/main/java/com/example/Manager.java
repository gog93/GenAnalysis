package com.example;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Manager {
    private GenAnalysis genAnalysis=new GenAnalysis();
    private Connection connection = DBConnectionProvider.getInstance().getConnection();

    public List<Symptom> getAll() {
        String sql = "SELECT * from sympt";
        List<Symptom> result = new ArrayList<>();
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                Symptom book = Symptom.builder()
                        .id(resultSet.getLong(1))
                        .sympt(resultSet.getString(2))
                        .build();
                result.add(book);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        GenAnalysis.doAnaliz(result);
        return result;
    }


}
