var interact = {
		
		/**
		 * execute the flow
		 * return instanceid.
		 */
		runFlow : function(flowid){
			var requestURL = "/dashview/{userName}/flow/";
			if(flowid == undefined || flowid == "") {
				return;
			}
			requestURL += flowid;
			$.ajax({
                type: "post",
                url: getAjaxAbsolutePath(requestURL),
                success: function(data, textStatus){
				  console.info(data);
				  return data;
                },
                error: function(e){
                	console.info(e);
                }
			});
		},
		
		/**
		 * common function for Ajax get request.
		 */
		ajaxGet : function(requestURL, dataParam, errorTips){
			var returnVal;
			if(dataParam == undefined || dataParam == ""){
				dataParam = {};
			}
			$.ajax({
                type: "get",
                async : false,
                url: getAjaxAbsolutePath(requestURL),
                data:dataParam,
                success: function(data, textStatus){
				  console.info(data);
				  returnVal = data;
//				  return data;
                },
                error: function(e){
                	console.info(e);
                	//TODO popup errorTips
                }
			});
			return returnVal;
		},
		
		saveFlowInstance : function(flowId, instanceid){
			var requestURL = "/dashview/{userName}/flow/{flowId}/instance/{instanceid}/add";
			if(isEmpty(flowId)  || isEmpty(instanceid)) {
				return;
			}
			requestURL = requestURL.replace("{flowId}",flowId);
			requestURL = requestURL.replace("{instanceid}",instanceid);
			return interact.ajaxGet(requestURL);
		},
		
		/**
		 * get flow instance information
		 * return json object as below:
		 * 		{bjectcreatedTime: "Tue, 20 Dec 2016 05:37:15 GMT
		 * 		"endTime: "Tue, 20 Dec 2016 05:37:45 GMT" 
		 * 		id:"0000076-161215094447342-oozie-dbad-W"
		 * 		lastModifiedTime: "Tue, 20 Dec 2016 05:37:45 GMT"
		 * 		name: "1"
		 * 		startTime: "Tue, 20 Dec 2016 05:37:15 GMT"
		 * 		status: "KILLED"}
		 * 
		 * 		status: "SUCCEEDED" / "RUNNING" / “FAILED' / "KILLED' / "SUSPENDED" 
		 */
		getFlowInstance : function(instanceid){
			var requestURL = "/dashview/{userName}/flow/instances/";
			if(instanceid == undefined || instanceid == "") {
				return;
			}
			requestURL += instanceid;
			return interact.ajaxGet(requestURL);
		},
		
		/**
		 * return the whole flow log information.
		 */
		getFlowLog : function(instanceid){
			var requestURL = "/dashview/{userName}/flow/instances/{instanceId}/log";
			if(instanceid == undefined || instanceid == "") {
				return;
			}
			requestURL = requestURL.replace("{instanceId}",instanceid);
			return interact.ajaxGet(requestURL);
		},
		
		/**
		 * return NodeInfo.
		 */
		getFlowNode : function(instanceid, nodeName){
			var requestURL = "/dashview/{userName}/flow/instances/{instanceId}/nodes/{nodeName}";
			if((instanceid == undefined || instanceid == "") && (nodeName == undefined || nodeName == "")) {
				return;
			}
			requestURL = requestURL.replace("{instanceId}",instanceid).replace("{nodeName}", nodeName);
			return interact.ajaxGet(requestURL);
		},
		
		/**
		 * get node log information.
		 * return InMemFile[].
		 */
		getFlowNodeLog : function(instanceid, nodeName){
			var requestURL = "/dashview/{userName}/flow/instances/{instanceId}/nodes/{nodeName}/log";
			if((instanceid == undefined || instanceid == "") && (nodeName == undefined || nodeName == "")) {
				return;
			}
			requestURL = requestURL.replace("{instanceId}",instanceid).replace("{nodeName}", nodeName);
			return interact.ajaxGet(requestURL);
		},
		
		/**
		 * get all properties type/valuelist/rule and so on.
		 */
		getFlowSchema : function(){
			var requestURL = "/dashview/{userName}/flow/schema";
			return interact.ajaxGet(requestURL);
		},
		
		/**
		 * get Flow defined content by flowid.
		 */
		getFlow : function(flowId){
			var requestURL = "/dashview/{userName}/flow/{flowId}";
			if(flowId == undefined || flowId == "") {
				return;
			}
			requestURL = requestURL.replace("{flowId}",flowId);
			return interact.ajaxGet(requestURL);
		},
		
		getFlowInstanceData : function(instanceid, dataName){
			var requestURL = "/dashview/{userName}/flow/instances/{instanceId}/data/{dataName:.+}";
			if(isEmpty(instanceid) && isEmpty(dataName)) {
				return;
			}
			requestURL = requestURL.replace("{instanceId}",instanceid).replace("{dataName:.+}", dataName);
			return interact.ajaxGet(requestURL);
		}
		
		
		


		
		
}