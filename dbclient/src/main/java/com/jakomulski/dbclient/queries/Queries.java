package com.jakomulski.dbclient.queries;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.jakomulski.dbclient.HSQLDatabase;

public class Queries {
	private static final ImmutableList<String> QUERIES = ImmutableList.of("SELECT pkey, name from STUDENT",
			"SELECT pkey, name from STUDENT WHERE pkey not in (SELECT DISTINCT fkey_student from ENROLLMENT)",
			"SELECT DISTINCT s.pkey, s.name FROM ENROLLMENT e JOIN CLASS c ON (e.fkey_class=c.pkey) JOIN STUDENT s ON (s.pkey = e.fkey_student) where c.pkey = 1002 AND s.sex = 'female'",
			"SELECT name FROM FACULTY where pkey not in(SELECT DISTINCT fkey_faculty FROM ENROLLMENT e JOIN CLASS c ON (e.fkey_class=c.pkey) JOIN STUDENT s ON (s.pkey = e.fkey_student))",
			"SELECT max(s.age) as max_age FROM ENROLLMENT e JOIN CLASS c ON (e.fkey_class=c.pkey) JOIN STUDENT s ON (s.pkey = e.fkey_student) where c.pkey = 1000",
			"SELECT c.name FROM ENROLLMENT e JOIN CLASS c ON (e.fkey_class=c.pkey) JOIN STUDENT s ON (s.pkey = e.fkey_student) GROUP BY c.name HAVING count(c.name) >= 2",
			"SELECT level, avg(age) as avg_age FROM STUDENT where level in (SELECT DISTINCT level FROM STUDENT) GROUP BY level");

	public static void executeAll() {
		int[] idx = { 0 };
		QUERIES.stream().map(Queries::mapQueryToOptionalResultSet).map(Queries::mapOptionalResultSetToString)
				.forEachOrdered(result -> {
					idx[0]++;
					System.out.println("Wynik zapytania " + idx[0] + ":");
					System.out.println(result);
				});
	}

	private static Optional<ResultSet> mapQueryToOptionalResultSet(String query) {
		try {
			return Optional.ofNullable(HSQLDatabase.INSTANCE.query(query));
		} catch (SQLException e) {
			e.printStackTrace();
			return Optional.empty();
		}
	}

	private static String mapOptionalResultSetToString(Optional<ResultSet> result) {
		return result.map(Queries::mapResultSetToString).orElse("no results");
	}

	private static String mapResultSetToString(ResultSet resultSet) {
		try {
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnsNumber = resultSet.getMetaData().getColumnCount();

			StringBuilder result = new StringBuilder();
			while (resultSet.next()) {
				for (int i = 1; i <= columnsNumber; i++) {
					if (i > 1)
						result.append(", ");
					String columnValue = resultSet.getString(i);
					result.append(metaData.getColumnName(i) + "=" + columnValue);
				}
				result.append('\n');
			}
			return result.toString();
		} catch (SQLException e) {
			e.printStackTrace();
			return "error";
		}
	}
}
