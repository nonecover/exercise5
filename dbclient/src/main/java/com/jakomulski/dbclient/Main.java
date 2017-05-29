package com.jakomulski.dbclient;

import com.jakomulski.dbclient.queries.Migration;
import com.jakomulski.dbclient.queries.Queries;

public class Main {

	public static void main(String[] args) {
		Migration.perform();
		Queries.executeAll();
	}
}
