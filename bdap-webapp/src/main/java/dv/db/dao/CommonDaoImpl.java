package dv.db.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.engine.spi.SessionImplementor;
import dv.tableau.bl.TableauBLImpl;
import dv.util.CommonDaoUtil;


public class CommonDaoImpl {
	public static final Logger logger = LogManager.getLogger(TableauBLImpl.class);
	
	@PersistenceContext  
    private EntityManager em; 
	public List<Map<String, Object>> getNativeMapBySQL(String sql){
		SessionImplementor session =em.unwrap(SessionImplementor.class);
		Connection connection =session.connection();
		List<Map<String, Object>> list =null;
		PreparedStatement  ps=null;
		ResultSet rs = null;
		try {
			  ps = connection.prepareStatement(sql);
			  rs = ps.executeQuery();
			  list = CommonDaoUtil.convertList(rs);
		} catch (SQLException e) {
			logger.error("", e);
		}finally{
				try {
					if(rs !=null){
						rs.close();
						rs=null;
					}
					if(ps!=null){
						ps.close();
						ps=null;
					}
					/*if(connection!=null){
						connection.close();
						connection=null;
					}*/
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
		logger.info(list);
		return list;
	}
}
