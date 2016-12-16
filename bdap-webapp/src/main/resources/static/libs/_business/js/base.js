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

		group.enter().append("g") //enter
			.each(function(d) {
				var nodeData = g.node(d);
				if(nodeData.action.toString().localeCompare("group") == 0) {
					groupEnter(this, d, nodeData);
				} else if(nodeData.action.toString().localeCompare("node") == 0) {
					actionEnter(this, d, nodeData);
				}
			});

		group.each(function(d) { //update
			var nodeData = g.node(d);
			if(nodeData.action.toString().localeCompare("group") == 0) {
				groupUpdate(this, d, nodeData);
			} else if(nodeData.action.toString().localeCompare("node") == 0) {
				actionUpdate(this, d, nodeData);
			}
		});

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
			if(!g.hasNode(d.v) || !g.hasNode(d.v)) {
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

				linegroup.append("circle")
					.attr("id", "outPath" + d.v + "A" + d.w)
					.attr("G", "linegroup" + d.v + d.w)
					.attr("self", "OutPath")
					.attr("class", "outPath")
					.attr("r", "4").attr("cx", points[0]["x"]).attr("cy", points[0]["y"]);

				var endx = points[points.length - 1]["x"];
				var endy = points[points.length - 1]["y"];

				linegroup.append("circle")
					.attr("id", "inPath" + d.v + "A" + d.w)
					.attr("G", "linegroup" + d.v + d.w)
					.attr("self", "InPath")
					.attr("class", "inPath")
					.attr("r", "3").attr("cx", endx).attr("cy", endy);

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

/**
 * actionEnter 子节点
 * @param {Object} theSelf
 * @param {Object} d
 * @param {Object} nodeData
 */
var actionEnter = function(theSelf, d, nodeData) {
	//var nodeData = g.node(d);
	console.log("-------------actionEnter:" + d + "--------------------");
	var tempM = "";
	var theSelfObj = d3.select(theSelf);
	var transform_x = 0;
	transform_x = (nodeData.x - nodeData.width / 2);
	console.log("transform_x",transform_x);
	theSelfObj.attr("id", d)
		.attr("class", "nodeG")
		.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)")
		.attr("onmousedown","node_mouse_down()")
		.attr("onmouseup","node_mouse_up()");
	
	theSelfObj.append("rect") //rect
		.attr("G", d)
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
		.attr("onclick","log_click()")
		.attr("cx", nodeData.width - 10)
		.attr("cy", nodeData.height - 10);		

	tempM = "M10," + (nodeData.height - 5);
	tempM = tempM + "L10," + (nodeData.height - 15);
	tempM = tempM + "L20," + (nodeData.height - 10) + " Z";

	theSelfObj.append("path") //path run
		.attr("G", d)
		.attr("class", "nodeRun")
		.attr("d", tempM);

	var theSelfObjChild = theSelfObj.append("g");
	theSelfObjChild.attr("G", d).attr("class", "minNodeG")
		.attr("onclick","zoom_click()")
		.attr("transform", "translate(" + (nodeData.width - 10) + ",10)scale(1,1)");

	theSelfObjChild.append("circle").attr("G", d)
		.attr("class", "minNodeCircle")
		.attr("self", "ShowProperty")
		.attr("r", 5);

	theSelfObjChild.append("path").attr("G", d)
		.attr("class", "minNodePath")
		.attr("self", "ShowProperty")
		.attr("d", "M-3,0L3,0M0,-3L0,3");

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

	tempM = "M10," + (nodeData.height - 5);
	tempM = tempM + "L10," + (nodeData.height - 15);
	tempM = tempM + "L20," + (nodeData.height - 10) + " Z";

	theSelfObj.select(".nodeRun") //path run
		.transition().duration(500)
		.attr("d", tempM);

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
		'outlets': '0',
		'nodes': [],
		'links': [],
		'data': [],
		'inlets': '0',
		'wfName': 'flow1'
	};

	each(nodeLists, function(i, o) {
		var nodekey = o.k;
		each(propertyList, function(index, obj) {
			if(obj.key.toString().localeCompare(nodekey.toString()) == 0) {
				var temp_o = {};
				each(obj.value, function(index_two, obj_two) {
					if(obj_two.k.toString().localeCompare("name") == 0) {
						temp_o[obj_two.k] = obj_two.v + "#" + nodekey;
						gIdAndgName.push({
							gId: nodekey,
							asName: obj_two.v + "#" + nodekey
						})
					} else {
						temp_o[obj_two.k] = obj_two.v;
					}

					return true;
				});
				result.nodes.push(temp_o);
				return false;
			}
			return true;
		});
		return true;
	});

	console.log("gIdAndgName", gIdAndgName);
	console.log("pathLists", pathLists);
	each(dataList, function(i, o) {
		o.v.name = o.v.name + "#" + o.k;
		result.data.push(o.v);
		return true;
	});
	each(pathLists, function(i, o) {
		var tempfrom = "";
		var tempto = "";
		each(gIdAndgName, function() {
			if(tempfrom.length > 0 && tempto.length > 0) {
				return false;
			}
			if(o.from.toString().localeCompare(this.gId) == 0) {
				tempfrom = this.asName;
			}
			if(o.to.toString().localeCompare(this.gId) == 0) {
				tempto = this.asName;
			}
			return true;
		});
		var tempObj = {
			fromNodeName: tempfrom,
			toNodeName: tempto,
			linkType: 'success'
		}
		result.links.push(tempObj);
		return true;
	});

	console.log(result);

	//	$.post("http://16.165.184.80:8080/dashview/george/flow", 
	//	headers: {'Content-type': 'application/json;charset=UTF-8'}	
	//	,{
	//		data: "txt"
	//	}, function(result) {
	//		$("span").html(result);
	//	});

	$.post("http://16.165.184.80:8080/dashview/george/flow", {
		data: "txt",
		headers: {
			'Content-type': 'application/json;charset=UTF-8'
		}
	}, function(result) {

	});


}

var setNodeSelf = function(keys, valueObj) {
	var json = dagre.graphlib.json.write(g);
	console.log(json);
	var nodeData = g.node(keys);
	if(nodeData) {
		//修改
		g.setNode(keys, {
			label: valueObj.label || nodeData.label,
			width: valueObj.width || 100,
			height: valueObj.height || 50,
			runstate: valueObj.runstate || 'play',
			action: valueObj.action || 'node',
			zoom: valueObj.zoom || 'normal'
		});
	} else {
		//添加
		g.setNode(keys, valueObj);
	}

	json = dagre.graphlib.json.write(g);
	console.log(json);

	var isadd = true;
	each(nodeLists, function() {
		if(this.k.toString().localeCompare(keys) == 0) {
			this.v = valueObj;
			isadd = false;
			return false;
		}else{
			return true;
		}
	});
	if(isadd) {
		nodeLists.push({
			k: keys,
			v: valueObj
		});
	}
	console.log("nodeLists",nodeLists);
}

/**
 * 
 * @param {Object} keys
 * @param {Object} valueObj
 */
var setPropertySelf = function(keys, valueObj){
	var isadd = true;
	each(propertyList,function(i,o){
		if(o.k.toString().localeCompare(keys)==0){
			o.v = valueObj;
			isadd = false;
			return false;
		}else{
			return true;
		}
	});
	if(isadd){
		propertyList.push({
			k: keys,
			v: valueObj
		})
	}
	console.log("propertyList",propertyList);
}

/**
 * 用于改变所有的样式
 * @param {Object} fromClassName
 * @param {Object} toClassName
 */
var allchangeClassNameForRect = function (fromClassName,toClassName){
	console.log("{}",d3.select("#rectContainer").selectAll("."+fromClassName));
	d3.select("#rectContainer").selectAll("."+fromClassName)
	.each(function(){
		console.log(d3.select(this));
		d3.select(this).attr("class",toClassName);
	});
}
