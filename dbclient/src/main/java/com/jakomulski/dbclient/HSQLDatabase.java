package com.jakomulski.dbclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public enum HSQLDatabase {
	INSTANCE;

	private static final String DRIVER = "org.hsqldb.jdbcDriver";
	private static final String URL = "jdbc:hsqldb:hsql://127.0.0.1:9001/test-db";
	private static final String USER = "SA";
	private static final String PASSWORD = "";

	private final Connection connection;

	private HSQLDatabase() {
		try {
			Class.forName(DRIVER);
			this.connection = DriverManager.getConnection(URL, USER, PASSWORD);
		} catch (ClassNotFoundException | SQLException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	public void update(String expression) throws SQLException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(expression);
			int result = statement.executeUpdate();
			if (result == -1) {
				System.out.println("db error : " + expression);
			}
		} finally {
			if (statement != null) {
				statement.close();
			}
		}

	}

	public ResultSet query(String expression) throws SQLException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(expression);
			return statement.executeQuery();
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
	}
}
