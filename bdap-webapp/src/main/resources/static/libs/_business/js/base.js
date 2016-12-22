var _base = {
	nodes: null,
	_build: function() {
		dagre.layout(g);
		nodes = g.nodes();
		nodes = this._sortNodes(g);

		this._groupNode(g);
		this._groupEdges();
	},
	/**
	 * 针对node进行排序，父节点排到前面，子节点排到后面，
	 * 防止渲染时父节点覆盖子节点（一般父节点矩形框大于子节点矩形框）
	 */
	_sortNodes: function(g) {
		var nodes = g.nodes();
		var newNodes = new Array();
		$.each(nodes, function(i, d) {
			//从当前节点取到所有父级节点（包含本节点）
			while($.inArray(d, newNodes) == -1) {
				_getTopParent(d, newNodes);
			}
		});
		return newNodes;

		function _getTopParent(name) {
			var arr = newNodes;
			if($.inArray(name, arr) != -1) {
				return "";
			}
			var parent = g.parent(name);
			if($.inArray(parent, arr) != -1) {
				newNodes.push(name);
				return name;
			}
			if(parent != undefined) {
				//递归
				_getTopParent(parent);
			} else {
				newNodes.push(name);
				return name;
			}
		}
	},

	/**
	 * 用于节点
	 */
	_groupNode: function(g) {

		var groupNode = d3.select("#rectContainer").selectAll(".nodeG");
		var group = groupNode.data(nodes, function(d) {
			return d;
		});

		var groupSmallNode = d3.select("#rectSmallContainer").selectAll(".nodeSmallG");
		var groupSmall = groupSmallNode.data(nodes, function(d) {
			return d;
		});

		groupSmall.enter().append("circle") //enter
			.each(function(d) {
				var nodeData = g.node(d);
				smallNodeEnter(this, d, nodeData);
			});

		group.enter().append("g") //enter
			.each(function(d) {
				var nodeData = g.node(d);
				if(nodeData.action.toString().localeCompare("group") == 0) {
					groupEnter(this, d, nodeData);
				} else if(nodeData.action.toString().localeCompare("node") == 0) {
					actionEnter(this, d, nodeData);
				}
			});

		groupSmall.each(function(d) {
			var nodeData = g.node(d);
			smallNodeUpdate(this, d, nodeData);
		});

		group.each(function(d) { //update
			var nodeData = g.node(d);
			if(nodeData.action.toString().localeCompare("group") == 0) {
				groupUpdate(this, d, nodeData);
			} else if(nodeData.action.toString().localeCompare("node") == 0) {
				actionUpdate(this, d, nodeData);
			}
		});

		groupSmall.exit()
			.each(function(d) {
				d3.select(this).remove();
				console.info("remove small node: " + d);
			})
			.remove();

		group.exit()
			.each(function(d) {
				d3.select(this).remove();
				console.info("remove node: " + d);
			})
			.remove();

	},
	/**
	 * 用于边界线
	 */
	_groupEdges: function() {
		d3.select("#lineContainer").selectAll(".edgeSelected").remove();
		var edges = g.edges();

		$.each(edges, function(i, d) {
			if(!g.hasNode(d.v) || !g.hasNode(d.w)) {
				g.removeEdge(d.v, d.w);
			}
		});

		var groupEdges = d3.select("#lineContainer").selectAll(".edge");
		var groupEdgesWithData = groupEdges.data(edges, function(d) {
			return d.v + "," + d.w;
		});

		groupEdgesWithData.enter()
			.append("g")
			.each(function(d) {
				var pro = g.edge(d);
				var points = pro.points;

				var linegroup = d3.select(this).attr("id", "linegroup" + d.v + "A" + d.w).attr("class", "edge");

				linegroup.append("path")
					.transition()
					.duration(500)
					.attr("id", "pathA" + d.v + "A" + d.w)
					.attr("G", "linegroup" + d.v + "A" + d.w)
					.attr("d", interpolate(points))
					.attr("marker-end", "url(#arrow)");

//				linegroup.append("circle")
//					.attr("id", "outPath" + d.v + "A" + d.w)
//					.attr("G", "linegroup" + d.v + d.w)
//					.attr("self", "OutPath")
//					.attr("class", "outPath")
//					.attr("r", "4").attr("cx", points[0]["x"]).attr("cy", points[0]["y"]);

				var endx = points[points.length - 1]["x"];
				var endy = points[points.length - 1]["y"];

//				linegroup.append("circle")
//					.attr("id", "inPath" + d.v + "A" + d.w)
//					.attr("G", "linegroup" + d.v + d.w)
//					.attr("self", "InPath")
//					.attr("class", "inPath")
//					.attr("r", "3").attr("cx", endx).attr("cy", endy);

			});

		groupEdgesWithData.each(function(d) {
			var pro = g.edge(d);
			var points = pro.points;
			var linegroup = d3.select(this);
			linegroup.select(".inPath")
				.transition()
				.duration(500)
				.attr("cx", points[0]["x"]).attr("cy", points[0]["y"]);

			var endx = points[points.length - 1]["x"];
			var endy = points[points.length - 1]["y"];
			linegroup.select(".outPath")
				.transition()
				.duration(500)
				.attr("cx", endx).attr("cy", endy);

			linegroup.select("path")
				.transition()
				.duration(500)
				.attr('d', interpolate(points));
		});

		groupEdgesWithData.exit()
			.each(function(d) {
				d3.select(this).remove();
			})
			.remove();
	}
}

var smallNodeEnter = function(theSelf, d, nodeData) {
	var theSelfObj = d3.select(theSelf);
	theSelfObj.attr("class", "nodeSmallG")
		.attr("id", "small" + d)
		.attr("r",2)
		.attr("cx", ((nodeData.x / 20) - 1))
		.attr("cy", ((nodeData.y / 20) - 1));
}

var smallNodeUpdate = function(theSelf, d, nodeData) {
	var theSelfObj = d3.select(theSelf);
	theSelfObj.attr("cx", ((nodeData.x / 20) - 1)).attr("cy", ((nodeData.y / 20) - 1));
}

var addInLetsButton = function(theSelfObj, d, nodeData){
	
	var theSelfObjChild = theSelfObj.append("g");
	theSelfObjChild.attr("G", d)
		.attr("class", "minNodeG inletsMinNodeG")
		.attr("self", "addInLetsPoint")
		.attr("transform", "translate(" + 10 + ",15)scale(1,1)");

	theSelfObjChild.append("circle").attr("G", d)
		.attr("class", "letsMinNodeCircle")
		.attr("self", "addInLetsPoint")
		.attr("r", 5);

	theSelfObjChild.append("path").attr("G", d)
		.attr("class", "letsMinNodePath")
		.attr("self", "addInLetsPoint")
		.attr("d", "M-3,0L3,0M0,-3L0,3");
}

var addOutLetsButton = function(theSelfObj, d, nodeData){
	
	var theSelfObjChild = theSelfObj.append("g");
	theSelfObjChild.attr("G", d).attr("class", "minNodeG outletsMinNodeG")
		.attr("self", "addOutLetsPoint")
		.attr("transform", "translate(" + 10 + ","+ (nodeData.height - 15) +")scale(1,1)");

	theSelfObjChild.append("circle").attr("G", d)
		.attr("class", "letsMinNodeCircle")
		.attr("self", "addOutLetsPoint")
		.attr("r", 5);

	theSelfObjChild.append("path").attr("G", d)
		.attr("class", "letsMinNodePath")
		.attr("self", "addOutLetsPoint")
		.attr("d", "M-3,0L3,0M0,-3L0,3");
}

var addInletsPoint = function(node, nodeData, g_mouse_down){
	var points = node.selectAll(".inPath");
	var length = points[0].length;
	if(length >= 8){
		return;
	}
	var number = 1;
	if(length > 0){
		var point = points[0][length-1]
		number = point.getAttribute("number");
		number = Number.parseInt(number);
		number += 1;
	}
	var x = length*18+25;
//	if(length == 0){
//		x += 20;
//	}
	
	node.append("circle")
		.attr("id", "inlets" + number)
		.attr("G", g_mouse_down)
		.attr("self", "inlets")
		.attr("class", "inPath")
		.attr("r", "4")
		.attr("number", number)
		.attr("transform", "translate(" + x + ",5)scale(1,1)");
	
}

var addOutletsPoint = function(node, nodeData, g_mouse_down){
	var points = node.selectAll(".outPath");
	var length = points[0].length;
	if(length >= 8){
		return;
	}
	var number = 1;
	if(length > 0){
		number = points[0][length-1].getAttribute("number");
		number = Number.parseInt(number);
		number += 1;
	}
	var x = length*18+25;
//	if(length == 0){
//		x += 20;
//	}
	node.append("circle")
		.attr("id", "outlets" + number)
		.attr("G", g_mouse_down)
		.attr("self", "outlets")
		.attr("class", "outPath")
		.attr("r", "4")
		.attr("number", number)
		.attr("transform", "translate(" + x + "," + (nodeData.height-5) + ")scale(1,1)");
	
}


/**
 * actionEnter 子节点
 * @param {Object} theSelf
 * @param {Object} d
 * @param {Object} nodeData
 */
var actionEnter = function(theSelf, d, nodeData) {
	//var nodeData = g.node(d);
	console.log("-------------actionEnter:" + d + "--------------------");
	console.log("nodeData:", nodeData);

	var tempM = "";
	var theSelfObj = d3.select(theSelf);
	var transform_x = 0;
	transform_x = (nodeData.x - nodeData.width / 2);

	theSelfObj.append("rect") //rect
		.attr("G", d)
		.attr("self", "RECT")
		.attr("width", nodeData.width)
		.attr("height", nodeData.height)
		.attr("rx", 5).attr("ry", 5);

	theSelfObj.append("text") //text
		.attr("G", d)
		.attr("id", "text_" + d)
		.attr("x", 20)
		.attr("y", 20).text(nodeData.label);

	theSelfObj.append("circle") //circle
		.attr("G", d)
		.attr("r", "5")
		.attr("self", "LOG")
		.attr("class", "nodeLog")
		.attr("cx", nodeData.width - 10)
		.attr("cy", nodeData.height - 10);

	tempM = "M"+ (nodeData.width-30) +"," + (nodeData.height - 5);
	tempM = tempM + "L"+ (nodeData.width-30) +"," + (nodeData.height - 15);
	tempM = tempM + "L"+ (nodeData.width-20) +"," + (nodeData.height - 10) + " Z";

	theSelfObj.append("path") //path run
		.attr("G", d)
		.attr("self", "RUN")
		.attr("class", "nodeRun")
		.attr("d", tempM);

	var theSelfObjChild = theSelfObj.append("g");
	theSelfObjChild.attr("G", d).attr("class", "minNodeG")
		.attr("self", "ShowProperty")
		.attr("transform", "translate(" + (nodeData.width - 10) + ",10)scale(1,1)");

	theSelfObjChild.append("circle").attr("G", d)
		.attr("class", "minNodeCircle")
		.attr("self", "ShowProperty")
		.attr("r", 5);

	theSelfObjChild.append("path").attr("G", d)
		.attr("class", "minNodePath")
		.attr("self", "ShowProperty")
		.attr("d", "M-3,0L3,0M0,-3L0,3");
	
	if(nodeData.nodeType.localeCompare("start") == 0) {
		theSelfObj.attr("id", d)
			.attr("class", "nodeG start")
			.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");
	} else if(nodeData.nodeType.localeCompare("end") == 0) {
		theSelfObj.attr("id", d)
			.attr("class", "nodeG end")
			.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");
	} else {
		theSelfObj.attr("id", d)
			.attr("class", "nodeG action")
			.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");
		
		addInLetsButton(theSelfObj, d, nodeData);
		addOutLetsButton(theSelfObj, d, nodeData);
	}


}



/**
 * groupEnter 子节点
 * @param {Object} theSelf
 * @param {Object} d
 * @param {Object} nodeData
 */
var groupEnter = function(theSelf, d, nodeData) {
	//var nodeData = g.node(d);
	console.log("-------------actionEnter:" + d + "--------------------");
	var tempM = "";
	var theSelfObj = d3.select(theSelf);
	theSelfObj.attr("id", d)
		.attr("class", "nodeG")
		.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");

	theSelfObj.append("rect") //rect
		.attr("G", d)
		.attr("class", "nodeRect")
		.attr("width", nodeData.width)
		.attr("height", nodeData.height)
		.attr("rx", 5).attr("ry", 5);

	theSelfObj.append("text") //text
		.attr("G", d)
		.attr("x", 10)
		.attr("y", 20).text(nodeData.label);

	theSelfObj.append("circle") //circle
		.attr("G", d)
		.attr("r", "5")
		.attr("self", "Log")
		.attr("class", "nodeLog")
		.attr("cx", nodeData.width - 10)
		.attr("cy", nodeData.height - 10);

	var theSelfObjChild = theSelfObj.append("g");
	theSelfObjChild.attr("G", d).attr("class", "minNodeG")
		.attr("transform", "translate(" + (nodeData.width - 10) + ",10)scale(1,1)");

	theSelfObjChild.append("circle").attr("G", d)
		.attr("class", "minNodeCircle")
		.attr("self", "ShowChildren")
		.attr("r", 5);

	theSelfObjChild.append("path").attr("G", d)
		.attr("class", "minNodePath")
		.attr("self", "ShowChildren")
		.attr("d", "M-3,0L3,0M0,-3L0,3");

}

/**
 *  actionUpdate 子节点
 * @param {Object} theSelf
 * @param {Object} d
 * @param {Object} nodeData
 */
var actionUpdate = function(theSelf, d, nodeData) {
	console.log("----------------actionUpdate:" + d + "----------------");
	console.log("nodeData:", nodeData);
	var tempM = "";
	var theSelfObj = d3.select(theSelf);
	theSelfObj.transition().duration(500)
		.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");

	theSelfObj.select("rect") //rect
		.transition().duration(500)
		.attr("width", nodeData.width)
		.attr("height", nodeData.height);

	theSelfObj.select("text").text(nodeData.label); //label

	theSelfObj.select(".nodeLog") //circle
		.transition().duration(500)
		.attr("cx", nodeData.width - 10)
		.attr("cy", nodeData.height - 10);

	tempM = "M"+ (nodeData.width-30) +"," + (nodeData.height - 5);
	tempM = tempM + "L"+ (nodeData.width-30) +"," + (nodeData.height - 15);
	tempM = tempM + "L"+ (nodeData.width-20) +"," + (nodeData.height - 10) + " Z";

	theSelfObj.select(".nodeRun") //path run
		.transition().duration(500)
		.attr("d", tempM);
	
	theSelfObj.select(".inletsMinNodeG")  //add inlets button
		.transition().duration(500)
		.attr("transform", "translate(" + 10 + ",15)scale(1,1)");
	
	theSelfObj.select(".outletsMinNodeG")   //add outlets button
	.transition().duration(500)
	.attr("transform", "translate(" + 10 + ","+ (nodeData.height - 15) +")scale(1,1)");
	
	var outnumber =0;
	theSelfObj.selectAll(".outPath")
			  .each(function(){
				  var x = outnumber*20+10;
					if(outnumber == 0){
						x += 20;
					}
					outnumber ++;
				  d3.select(this).transition().duration(500)
				   .attr("transform", "translate(" + x + "," + (nodeData.height-5) + ")scale(1,1)");
			  });
			  
	
	var theSelfObjChild = theSelfObj.select("g");
	theSelfObjChild.attr("transform", "translate(" + (nodeData.width - 10) + ",10)scale(1,1)");
	if(nodeData.zoom.localeCompare("normal") == 0) {
		theSelfObjChild.attr("class", "minNodeG");

		theSelfObjChild.select("circle")
			.transition().duration(500).attr("G", d)
			.attr("class", "minNodeCircle")
			.attr("self", "ShowProperty")
			.attr("r", 5);

		theSelfObjChild.select("path")
			.transition().duration(500).attr("G", d)
			.attr("class", "minNodePath")
			.attr("self", "ShowProperty")
			.attr("d", "M-3,0L3,0M0,-3L0,3");

	} else if(nodeData.zoom.localeCompare("max") == 0) {

		theSelfObjChild.attr("class", "maxNodeG");

		theSelfObjChild.select("circle")
			.transition().duration(500).attr("G", d)
			.each('end', function() {
				theSelfObjChild.select("path")
					.transition().duration(200)
					.each('end', function() {
						//loadProperty(d, nodeData);
					})
					.attr("G", d)
					.attr("class", "maxNodePath")
					.attr("self", "HideProperty")
					.attr("d", "M-3,0L3,0");
			})
			.attr("class", "maxNodeCircle")
			.attr("self", "HideProperty")
			.attr("r", 5);

		countProperty++;
	}
}

/**
 *  groupUpdate 子节点
 * @param {Object} theSelf
 * @param {Object} d
 * @param {Object} nodeData
 */
var groupUpdate = function(theSelf, d, nodeData) {
	console.log("----------------actionUpdate:" + d + "----------------");
	var tempM = "";
	var theSelfObj = d3.select(theSelf);
	theSelfObj.transition().duration(500)
		.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");

	theSelfObj.select("rect") //rect
		.transition().duration(500)
		.attr("width", nodeData.width)
		.attr("height", nodeData.height);

	theSelfObj.select(".nodeLog") //circle
		.transition().duration(500)
		.attr("cx", nodeData.width - 10)
		.attr("cy", nodeData.height - 10);

	var theSelfObjChild = theSelfObj.select("g");
	theSelfObjChild.attr("transform", "translate(" + (nodeData.width - 10) + ",10)scale(1,1)");
	if(nodeData.zoom.localeCompare("normal") == 0) {
		theSelfObjChild.attr("class", "minNodeG");

		theSelfObjChild.select("circle")
			.transition().duration(500).attr("G", d)
			.attr("class", "minNodeCircle")
			.attr("self", "ShowChildren")
			.attr("r", 5);

		theSelfObjChild.select("path")
			.transition().duration(500).attr("G", d)
			.attr("class", "minNodePath")
			.attr("self", "ShowChildren")
			.attr("d", "M-3,0L3,0M0,-3L0,3");

	} else if(nodeData.zoom.localeCompare("max") == 0) {

		theSelfObjChild.attr("class", "maxNodeG");

		theSelfObjChild.select("circle")
			.transition().duration(500).attr("G", d)
			.attr("class", "maxNodeCircle")
			.attr("self", "HideChildren")
			.attr("r", 5);

		theSelfObjChild.select("path")
			.transition().duration(500).attr("G", d)
			.attr("class", "maxNodePath")
			.attr("self", "HideChildren")
			.attr("d", "M-3,0L3,0");
	}
}

/**
 * 保存生成JSON
 * 
 * 	id,@class,name,inletNum
 * }
 */
var saveAsJson = function() {
	var gIdAndgName = [];
	var result = {
		'@class': 'flow',
		'name': 'flow1',
		'outlets': [],
		'nodes': [],
		'links': [],
		'data': [],
		'inLets': [],
		'wfName': 'flow1'
	};
	each(nodeLists, function() {
		var tempkeys = this.k.toString();
		var propertyObj = {};
		each(propertyList, function() {
			if(this.k.toString().localeCompare(tempkeys) == 0) {
				propertyObj = this.v;
				if(propertyObj.name.indexOf("g_") == -1){
					propertyObj.name = propertyObj.name + tempkeys;
				}
				result.nodes.push(propertyObj);
				return false;
			} else {
				return true;
			}
		});
		return true;
	});

	//	result.links = pathLists;

	each(pathLists, function(i, o) {
		var tempfromNodeName = o.fromNodeName;
		var temptoNodeName = o.toNodeName;
		
		each(propertyList, function() {
			if(tempfromNodeName.localeCompare(this.k.toString()) == 0) {
				o.fromNodeName = this.v.name;
			}
			if(temptoNodeName.localeCompare(this.k.toString()) == 0) {
				o.toNodeName = this.v.name;
			}
			return true;
		});
		return true;
	});

	result.links = pathLists;
	console.log("result", JSON.stringify(result));

	$.ajax({
		type: "post",
		url: getAjaxAbsolutePath(_HTTP_SAVE_JSON),
		contentType: 'application/json',
		data: JSON.stringify(result),
		//dataType: "json",
		success: function(data, textStatus) {
			console.info(data);
		},
		error: function(e) {
			console.info(e);
		}
	});

}

var setNodeSelf = function(keys, valueObj) {
	var nodeData = g.node(keys);
	if(nodeData) {
		//修改
		g.setNode(keys, {
			label: valueObj.label || nodeData.label,
			width: valueObj.width || 100,
			height: valueObj.height || 50,
			nodeType: valueObj.nodeType,
			runstate: valueObj.runstate || 'play',
			action: valueObj.action || 'node',
			zoom: valueObj.zoom || 'normal'
		});
	} else {
		//添加
		g.setNode(keys, valueObj);
	}
	var isadd = true;
	each(nodeLists, function() {
		if(this.k.toString().localeCompare(keys) == 0) {
			this.v = valueObj;
			isadd = false;
			return false;
		} else {
			return true;
		}
	});
	if(isadd) {
		nodeLists.push({
			k: keys,
			v: valueObj
		});
	}
}

/**
 * 
 * @param {Object} keys
 * @param {Object} valueObj
 * @param {Object} length
 */
var setPropertySelf = function(keys, valueObj, length) {
	var isadd = true;
	each(propertyList, function(i, o) {
		if(o.k.toString().localeCompare(keys) == 0) {
			o.v = valueObj;
			isadd = false;
			return false;
		} else {
			return true;
		}
	});
	if(isadd) {
		propertyList.push({
			k: keys,
			v: valueObj,
			length: length
		})
	}
}

/**
 * 用于改变所有的样式
 * @param {Object} fromClassName
 * @param {Object} toClassName
 */
var allchangeClassNameForRect = function(fromClassName, toClassName) {
	d3.select("#rectContainer").selectAll("." + fromClassName)
		.each(function() {
			d3.select(this).attr("class", toClassName);
		});
}

/**
 * 确定其最终的样式
 * @param {Object} keys
 */
var beSureClassName = function(keys) {
	var nodeData = g.node(keys);
	d3.select("#" + keys).attr("class", "nodeG " + nodeData.nodeType);
}

/**
 * 移除选定的样式
 */
var removeAllSelectedClass = function() {
	d3.select("#rectContainer").selectAll(".nodeG")
		.each(function() {
			var tempId = d3.select(this).attr("id");
			var nodeData = g.node(tempId);
			d3.select(this).attr("class", "nodeG " + nodeData.nodeType);
		});
}

/**
 * 移动到初始化的位置
 */
var remoeToInitPosition = function(){
	d3.select("#main").attr("transform", "translate(" + init_zoom_x + "," + init_zoom_y + ")scale(1,1)");
	d3.select("#rectSmallContainer").attr("transform", "translate(75,75)scale(1,1)");
	current_zoom_x = init_zoom_x;
	current_zoom_y = init_zoom_y;
}
