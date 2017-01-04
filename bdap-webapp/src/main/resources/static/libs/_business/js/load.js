var isLoaded = false;
var load = {
	isEmpty: function(str) {
		if(str == undefined || str == null || str == '') {
			return true;
		}
		return false;
	},
	loadBuild: function(flowid) {
		if(isLoaded) {
			return;
		}
		CONST_DURATION = 10;
		flowid = "flow1";
		var flowObj = interact.getFlow(flowid);
		console.log("flowObj",flowObj);
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
		isLoaded = true;
	},
	buildNodes: function(nodes) {
		var loadThis = this;
		$.each(nodes, function(i, obj) {
			if(obj['@class'] == 'start') {
				var id = obj.name;
				id = id.substring(id.indexOf("g_"));
				app.start(id);
			} else if(obj['@class'] == 'end') {
				var id = obj.name;
				id = id.substring(id.indexOf("g_"));
				app.end(id);
			} else if(obj['@class'] == 'action') {
				var id = obj.id;
				id = id.substring(id.indexOf("g_"));
				var cmdClass = obj['cmd.class'];
				var temp = cmdClass;
				temp = temp.substring(temp.lastIndexOf(".") + 1);
				var jsonData = {};
				jsonData.label = obj.name;
				jsonData.cla = cmdClass;
				//build action node
				app.action(jsonData, id);
				//build inlets/outlets
				loadThis.buildInAndOutLets(id, obj);
			}
			loadThis.loadProperties(obj);

		});
	},
	buildLinks: function(links) {
		$.each(links, function(i, link) {
			debugger
			var fromName = link.fromNodeName;
			var toName = link.toNodeName;
			fromName = fromName.substring(fromName.indexOf("g_"));
			toName = toName.substring(toName.indexOf("g_"));
			g.setEdge(fromName, toName);
			//pathLists.push(link);
		});
		_build._build();

	},
	buildInAndOutLets: function(id, nodeParam) {
		var nodeData = g.node(id);
		var node = d3.selectAll("#" + id);
		var inLets = nodeParam.inLets;
		var outLets = nodeParam.outlets;
		var addInletButtonThis = document.getElementById("addIn_circle_" + id);
		$.each(inLets, function(i, inlet) {
			_event.data_addIn_click(addInletButtonThis);
		});
		$.each(outLets, function(i, outlet) {
			_event.data_addOut_click(addInletButtonThis);
		});
		_draw._drawInputData(id);
		_draw._drawOutputData(id);
	},
	loadProperties: function(obj) {
		var inlets = obj.inLets;
		var outlets = obj.outlets;
		var nodeList = [];
		$.each(result.nodes, function(i, node) {
			if(node.id == obj.id) {
				console.log("--------------1--------------")
				console.log(obj.inLets);
				console.log(node.inLets);
				console.log("--------------2--------------")
				var newInletList = [];
				var newOutletList = [];
				var inNum = -1;
				var outNum =-1;
				$.each(obj.inLets, function(j, inlet) {
//					delete inlet.name;
//					delete inlet.dataName;
					var newInLet =$.extend({},  node.inLets[j],inlet);
					newInletList.push(newInLet);
					inNum = j;
				});
				$.each(obj.outlets, function(j, outlet) {
					var newOutlet = $.extend({}, node.outlets[j],outlet);
					newOutletList.push(newOutlet);
					outNum =j;
				});
				while(inNum <3){
					inNum ++;
					newInletList.push(node.inLets[inNum]);
				}
				while(outNum <3){
					outNum ++;
					newOutletList.push(node.outlets[outNum]);
				}
//				var newInLets = $.extend([], obj.inLets, node.inLets);
//				var newOutlets = $.extend([], obj.outlets, node.outlets);
				obj.inLets = newInletList;
				obj.outlets = newOutletList;
				var newNode = $.extend({}, node, obj);
				nodeList.push(newNode);
			} else {
				nodeList.push(node);
			}

		});
		result.nodes = nodeList;
		console.log("result",result);
	}

}