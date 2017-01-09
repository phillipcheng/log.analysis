var run = {
		changeNodeStyle : function(nodeName, state){
			$.each(FLOW_RUNTIME_STATE, function(i, obj){
				if(obj.state == state) {
					var fillColor = "fill:" + obj.color;
					var test = $("#"+nodeName).attr("class");
					console.info(test);
					d3.select("#"+nodeName).attr("class","");
					d3.select("#"+nodeName).attr("style", fillColor);
				}
			});
		},
		
		runFlow : function(flowname){
			var thisObj = this;
			if(isEmpty(flowname)){
				flowname = "flow1";
			}
			
			var flowid = flowname;
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
				  var instanceid = data;
				  if(isEmpty(instanceid)){
						console.info("run fail.");
						return;
					}
					// save instance
					interact.saveFlowInstance(flowname, instanceid);
					//open websocket
					thisObj.connectWebSoket(instanceid);
                },
                error: function(e){
                	console.info(e);
                }
			});
			
//			var instanceid = interact.runFlow(flowname);
			
		},
		
		connectWebSoket : function(instanceid){
			if(instanceid == undefined || $.trim(instanceid) == ''){
				return;
			}
			//test id
			//instanceid = "0000277-170104195305044-oozie-dbad-W";
			wsURI += instanceid;
			var websocket;
			var thisObj = this;
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
	        	thisObj.messageHandler(websocket, "error");
	        };

	        //连接成功建立的回调方法
	        websocket.onopen = function(event){
	        	thisObj.messageHandler(websocket, "open");
	        }

	        //接收到消息的回调方法
	        websocket.onmessage = function(event){
	        	thisObj.messageHandler(websocket, event.data);
	        }

	        //连接关闭的回调方法
	        websocket.onclose = function(){
	        	thisObj.messageHandler(websocket, "close");
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