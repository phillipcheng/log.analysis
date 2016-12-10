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
					.attr("class", "outPath")
					.attr("r", "4").attr("cx", points[0]["x"]).attr("cy", points[0]["y"]);

				var endx = points[points.length - 1]["x"];
				var endy = points[points.length - 1]["y"];

				linegroup.append("circle")
					.attr("id", "inPath" + d.v + "A" + d.w)
					.attr("G", "linegroup" + d.v + d.w)
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
	console.log("-------------actionEnter:"+d+"--------------------");
	var tempM = "";
	var theSelfObj = d3.select(theSelf);
	theSelfObj.attr("id", d)
		.attr("class", "nodeG")
		.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");

	theSelfObj.append("rect") //rect
		.attr("G", d)
		.attr("class","nodeRect")
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

	tempM = "M10," + (nodeData.height - 5);
	tempM = tempM + "L10," + (nodeData.height - 15);
	tempM = tempM + "L20," + (nodeData.height - 10) + " Z";

	theSelfObj.append("path") //path run
		.attr("G", d)
		.attr("class", "nodeRun")
		.attr("d", tempM);

	var theSelfObjChild = theSelfObj.append("g");
	theSelfObjChild.attr("G", d).attr("class", "minNodeG")
		.attr("transform", "translate(" + (nodeData.width - 10) + ",10)scale(1,1)");

	theSelfObjChild.append("circle").attr("G", d)
		.attr("class","minNodeCircle")
		.attr("self", "ShowProperty")
		.attr("r", 5);

	theSelfObjChild.append("path").attr("G", d)
		.attr("class","minNodePath")
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
	console.log("-------------actionEnter:"+d+"--------------------");
	var tempM = "";
	var theSelfObj = d3.select(theSelf);
	theSelfObj.attr("id", d)
		.attr("class", "nodeG")
		.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");

	theSelfObj.append("rect") //rect
		.attr("G", d)
		.attr("class","nodeRect")
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
		.attr("class","minNodeCircle")
		.attr("self", "ShowChildren")
		.attr("r", 5);

	theSelfObjChild.append("path").attr("G", d)
		.attr("class","minNodePath")
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
	console.log("----------------actionUpdate:"+d+"----------------");
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
		tempM = tempM + "L20,"+ (nodeData.height - 10) + " Z";
	
		theSelfObj.select(".nodeRun") //path run
		.transition().duration(500)
			.attr("d", tempM);	

	var theSelfObjChild = theSelfObj.select("g");
	theSelfObjChild.attr("transform", "translate(" + (nodeData.width - 10) + ",10)scale(1,1)");
	if(nodeData.zoom.localeCompare("normal") == 0) {
		theSelfObjChild.attr("class", "minNodeG");

		theSelfObjChild.select("circle")
		.transition().duration(500).attr("G", d)
			.attr("class","minNodeCircle")
			.attr("self", "ShowProperty")
			.attr("r", 5);
			
		theSelfObjChild.select("path")
		.transition().duration(500).attr("G", d)
			.attr("class","minNodePath")
			.attr("self", "ShowProperty")
			.attr("d", "M-3,0L3,0M0,-3L0,3");			
			
	} else if(nodeData.zoom.localeCompare("max") == 0) {
		
		theSelfObjChild.attr("class", "maxNodeG");

		theSelfObjChild.select("circle")
		.transition().duration(500).attr("G", d)
			.attr("class","maxNodeCircle")
			.attr("self", "HideProperty")
			.attr("r", 5);
			
		theSelfObjChild.select("path")
		.transition().duration(500).attr("G", d)
			.attr("class","maxNodePath")
			.attr("self", "HideProperty")
			.attr("d", "M-3,0L3,0");	
	}
}

/**
 *  groupUpdate 子节点
 * @param {Object} theSelf
 * @param {Object} d
 * @param {Object} nodeData
 */
var groupUpdate = function(theSelf, d, nodeData) {
	console.log("----------------actionUpdate:"+d+"----------------");
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

//		tempM = "M10," + (nodeData.height - 5);
//		tempM = tempM + "L10," + (nodeData.height - 15);
//		tempM = tempM + "L20,"+ (nodeData.height - 10) + " Z";
//	
//		theSelfObj.select(".nodeRun") //path run
//		.transition().duration(500)
//			.attr("d", tempM);	

	var theSelfObjChild = theSelfObj.select("g");
	theSelfObjChild.attr("transform", "translate(" + (nodeData.width - 10) + ",10)scale(1,1)");
	if(nodeData.zoom.localeCompare("normal") == 0) {
		theSelfObjChild.attr("class", "minNodeG");

		theSelfObjChild.select("circle")
		.transition().duration(500).attr("G", d)
			.attr("class","minNodeCircle")
			.attr("self", "ShowChildren")
			.attr("r", 5);
			
		theSelfObjChild.select("path")
		.transition().duration(500).attr("G", d)
			.attr("class","minNodePath")
			.attr("self", "ShowChildren")
			.attr("d", "M-3,0L3,0M0,-3L0,3");			
			
	} else if(nodeData.zoom.localeCompare("max") == 0) {
		
		theSelfObjChild.attr("class", "maxNodeG");

		theSelfObjChild.select("circle")
		.transition().duration(500).attr("G", d)
			.attr("class","maxNodeCircle")
			.attr("self", "HideChildren")
			.attr("r", 5);
			
		theSelfObjChild.select("path")
		.transition().duration(500).attr("G", d)
			.attr("class","maxNodePath")
			.attr("self", "HideChildren")
			.attr("d", "M-3,0L3,0");	
	}
}
