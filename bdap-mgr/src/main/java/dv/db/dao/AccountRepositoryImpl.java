package dv.db.dao;


import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import dv.db.entity.AccountEntity;


public class AccountRepositoryImpl {
	
	@PersistenceContext  
    private EntityManager em; 
	public Page<Object[]> getNativeByCondition(Map map){
		String sql = "select * from t_Account o where o.userid= :userid";
		Query q = em.createNativeQuery(sql, AccountEntity.class);
		q.setParameter("userid", map.get("userid")); 
		List list = q.getResultList();
		Page<Object[]> page = new PageImpl<Object[]>(list, new PageRequest(0,10),list.size());   
		System.out.println(page);
		System.out.println("自定义sql查询");
        return page;  
		
	}

	

}
