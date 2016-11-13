package dv.mgr;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import bdap.util.JsonUtil;
import dv.UserNotFoundException;
import dv.db.dao.AccountRepository;
import dv.db.dao.FlowRepository;
import dv.db.entity.AccountEntity;
import dv.db.entity.FlowEntity;
import etl.flow.Flow;

@RestController
@RequestMapping("/{userName}/flow")
public class FlowController {
	
	@Autowired
	FlowController(FlowRepository flowRepository, AccountRepository accountRepository) {
		this.flowRepository = flowRepository;
		this.accountRepository = accountRepository;
	}
	
	private final FlowRepository flowRepository;
	private final AccountRepository accountRepository;
	
	@RequestMapping(method = RequestMethod.POST)
	ResponseEntity<?> add(@PathVariable String userName, @RequestBody Flow input) {
		this.validateUser(userName);
		FlowEntity fe = flowRepository.save(new FlowEntity(input.getName(),
				userName, JsonUtil.toJsonString(input)));

		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.setLocation(ServletUriComponentsBuilder
				.fromCurrentRequest().path("/{id}")
				.buildAndExpand(fe.getName()).toUri());
		return new ResponseEntity<>(null, httpHeaders, HttpStatus.CREATED);
	}
	
	@RequestMapping(value = "/{flowId}", method = RequestMethod.GET)
	FlowEntity getFlow(@PathVariable String userName, @PathVariable String flowId) {
		this.validateUser(userName);
		return this.flowRepository.findOne(flowId);
	}
	
	private void validateUser(String userName) {
		AccountEntity ae = this.accountRepository.findOne(userName);
		if (ae==null){
			throw new UserNotFoundException(userName);
		}
	}
	
}
