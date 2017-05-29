package wdsr.exercise5.dao;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.util.Optional;

import org.hsqldb.types.Types;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import wdsr.exercise5.model.Trade;

@Repository
public class TradeDao {

	@Autowired
	JdbcTemplate jdbcTemplate;

	/**
	 * Zaimplementuj metode insertTrade aby wstawiała nowy rekord do tabeli
	 * "trade" na podstawie przekazanego objektu klasy Trade.
	 * 
	 * @param trade
	 * @return metoda powinna zwracać id nowego rekordu.
	 */
	public int insertTrade(Trade trade) {
		String sql = "INSERT INTO trade(asset, amount, date) values(?, ?, ?)";

		KeyHolder keyHolder = new GeneratedKeyHolder();
		jdbcTemplate.update(con -> {
			PreparedStatement preparedStatement = con.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			preparedStatement.setString(1, trade.getAsset());
			preparedStatement.setDouble(2, trade.getAmount());
			preparedStatement.setDate(3, new Date(trade.getDate().getTime()));
			return preparedStatement;
		}, keyHolder);

		return keyHolder.getKey().intValue();
	}

	/**
	 * Zaimplementuj metode aby wyciągneła z bazy rekord o podanym id. Użyj
	 * intrfejsu RowMapper.
	 * 
	 * @param id
	 * @return metaoda powinna zwracać obiekt reprezentujący rekord o podanym
	 *         id.
	 */
	public Optional<Trade> extractTrade(int id) {
		String sql = "SELECT * FROM TRADE WHERE id = ?";
		Trade trade = jdbcTemplate.queryForObject(sql, new Object[] { id }, tradeRowMapper);
		return Optional.ofNullable(trade);
	}

	private final RowMapper<Trade> tradeRowMapper = (resultSet, rowNum) -> new Trade(resultSet.getInt("id"),
			resultSet.getString("asset"), resultSet.getDouble("amount"), resultSet.getDate("date"));

	/**
	 * Zaimplementuj metode aby wyciągneła z bazy rekord o podanym id.
	 * 
	 * @param id,
	 *            rch - callback który przetworzy wyciągnięty wiersz.
	 * @return metaoda powinna zwracać obiekt reprezentujący rekord o podanym
	 *         id.
	 */
	public void extractTrade(int id, RowCallbackHandler rch) {
		String sql = "SELECT * FROM TRADE WHERE id=" + id;
		jdbcTemplate.query(sql, rch);
	}

	/**
	 * Zaimplementuj metode aby zaktualizowała rekord o podanym id danymi z
	 * przekazanego parametru 'trade'
	 * 
	 * @param trade
	 */
	public void updateTrade(int id, Trade trade) {
		String sql = "UPDATE TRADE SET asset = ?, amount = ?, date = ? WHERE ID = ?";
		jdbcTemplate.update(sql, new Object[] { trade.getAsset(), trade.getAmount(), trade.getDate(), id },
				new int[] { Types.VARCHAR, Types.DOUBLE, Types.DATE, Types.INTEGER });
	}

	/**
	 * Zaimplementuj metode aby usuwała z bazy rekord o podanym id.
	 * 
	 * @param id
	 */
	public void deleteTrade(int id) {
		String sql = "DELETE FROM TRADE WHERE id = ?";
		jdbcTemplate.update(sql, id);
	}

}
