package dv.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CommonDaoUtil {
	public static final Logger logger = LogManager.getLogger(CommonDaoUtil.class);
	public static List<Map<String, Object>> convertList(ResultSet rs) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			ResultSetMetaData md = rs.getMetaData();
			int columnCount = md.getColumnCount();
			while (rs.next()) {
				Map<String, Object> rowData = new HashMap<String, Object>();
				for (int i = 1; i <= columnCount; i++) {
					rowData.put(md.getColumnName(i).toLowerCase(),
							rs.getObject(i));
				}
				list.add(rowData);
			}
		} catch (SQLException e) {
			logger.error("", e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				rs = null;
			} catch (SQLException e) {
				logger.error("", e);
			}
		}
		return list;
	}
}
