var isLoaded = false;
var load = {
		isEmpty : function(str) {
			if(str == undefined || str == null || str == '') {
				return true;
			}
			return false;
		},
		loadBuild : function(flowid){
			if(isLoaded){
				return;
			}
			CONST_DURATION = 10;
			flowid = "flow1";
			var flowObj = interact.getFlow(flowid);
			if(!this.isEmpty(flowObj)) {
				if(this.isEmpty(flowObj.jsonContent)) {
					return;
				}
				var flowContext = JSON.parse(flowObj.jsonContent);
				var nodes = flowContext.nodes;
				var links = flowContext.links;
				var datas = flowContext.data;
				
				//load nodes
				this.buildNodes(nodes);
				//load links
				this.buildLinks(links);
				
			}
			CONST_DURATION = 500;
			isLoaded = true;
		},
		buildNodes : function(nodes){
			var loadThis = this;
			$.each(nodes, function(i, obj){
				var name = obj.name;
				name = name.substring(name.indexOf("g_"));
				if(obj['@class'] == 'start'){
					app.start(name);
				}else if(obj['@class'] == 'end'){
					app.end(name);
				}else if(obj['@class'] == 'action'){
					var cmdClass = obj['cmd.class'];
					var temp = cmdClass;
					temp = temp.substring(temp.lastIndexOf(".") + 1);
					var jsonData = {};
					jsonData.label = temp;
					jsonData.class = cmdClass;
					//build action node
					app.action(jsonData, name);
					//build inlets/outlets
					loadThis.buildInAndOutLets(name, obj);
					loadThis.loadProperties(name, obj);
				}
				
			});
		},
		buildLinks : function(links){
			$.each(links, function(i, link){
				var fromName = link.fromNodeName;
				var toName = link.toNodeName;
				fromName = fromName.substring(fromName.indexOf("g_"));
				toName = toName.substring(toName.indexOf("g_"));
				g.setEdge(fromName, toName);
				pathLists.push(link);
			});
			_base._build();
			
		},
		buildInAndOutLets : function(name, nodeParam){
			var nodeData = g.node(name);
			var node = d3.selectAll("#" + name);
			var inLets = nodeParam.inLets;
			var outLets = nodeParam.outlets;
			$.each(inLets, function(i, inlet){
				addInletsPoint(node, nodeData, name);
			});
			$.each(outLets, function(i, inlet){
				addOutletsPoint(node, nodeData, name);
			});
		},
		loadProperties : function(name, obj){
			var propertyObj = {};
			propertyObj.k = name;
			propertyObj.v = obj;
			var isFinded = false;
			$.each(propertyList, function(i, property){
				if(property.k == name){
					property.v = obj;
					isFinded = true;
				}
			});
			if(!isFinded){
				propertyList.push(propertyObj);
			}
		}
		
}