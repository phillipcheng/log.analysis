var run = {
		changeNodeStyle : function(nodeName, state){
			$.each(FLOW_RUNTIME_STATE, function(i, obj){
				if(obj.state == state) {
					var fillColor = "fill:" + obj.color;
					d3.select("#"+nodeName).attr("style", fillColor);
				}
			});
		},
		
		runFlow : function(flowname){
			var thisObj = this;
			if(isEmpty(flowname)){
				flowname = "flow1";
			}
			var instanceid = interact.runFlow(flowname);
			if(isEmpty(instanceid)){
				console.info("run fail.");
				return;
			}
			// save instance
			interact.saveFlowInstance(flowname, instanceid);
			//open websocket
			thisObj.connectWebSoket(instanceid);
		},
		
		connectWebSoket : function(instanceid){
			if(instanceid == undefined || $.trim(instanceid) == ''){
				return;
			}
			//test id
			instanceid = "0000266-161215094447342-oozie-dbad-W";
			wsURI += instanceid;
			var websocket;
	        //判断当前浏览器是否支持WebSocket
	        if('WebSocket' in window){
	            websocket = new WebSocket(wsURI);
	        }
	        else{
	            alert('Not support websocket')
	        }
	        if(websocket == null) return;

	        //连接发生错误的回调方法
	        websocket.onerror = function(){
	        	messageHandler(websocket, "error");
	        };

	        //连接成功建立的回调方法
	        websocket.onopen = function(event){
	        	messageHandler(websocket, "open");
	        }

	        //接收到消息的回调方法
	        websocket.onmessage = function(event){
	        	messageHandler(websocket, event.data);
	        }

	        //连接关闭的回调方法
	        websocket.onclose = function(){
	        	messageHandler(websocket, "close");
	        }

	        //监听窗口关闭事件，当窗口关闭时，主动去关闭websocket连接，防止连接还没断开就关闭窗口，server端会抛异常。
	        window.onbeforeunload = function(){
	            websocket.close();
	        }
	    },
	    messageHandler : function(websocket, str){
			console.info(str);
			var jsonResult =  JSON.parse(str);
			
			var instanceid = jsonResult.instanceId;
			var nodeName = jsonResult.nodeName;
			var status = jsonResult.status;
			if(!isEmpty(nodeName)){
				this.changeNodeStyle(nodeName, status);
			}else{
				if(status == 'KILLED' || status == 'FAILED') {
					websocket.close();
				}
			}
			
		}


}