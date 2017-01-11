var _build = {
	nodes: null,
	_build: function(gId, g_child_Instance) {
		if(gId) {
			dagre.layout(g_child_Instance);
			nodes = g_child_Instance.nodes();
			nodes = this._sortNodes(g_child_Instance);
			this._groupNodeChild(gId, g_child_Instance);
		} else {
			dagre.layout(g);
			nodes = g.nodes();
			nodes = this._sortNodes(g);
			this._groupNode(g);
			this._groupEdges();
			_draw._drawNodeDataRelation();
		}
		clearTempLine();
	},
	/**
	 * 针对node进行排序，父节点排到前面，子节点排到后面，
	 * 防止渲染时父节点覆盖子节点（一般父节点矩形框大于子节点矩形框）
	 */
	_sortNodes: function(g) {
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

	_groupNodeChild: function(gId, g_Instance) {
		var groupChildNode = d3.select("#" + gId)
			.select("#" + gId + "_g_svg")
			.select("#rectChildContainer").selectAll(".nodeChildG");

		var groupChild = groupChildNode.data(nodes, function(d) {
			return d;
		});

		groupChild.enter().append("g") //enter
			.each(function(d) {
				var nodeData = g_Instance.node(d);
				childNodeEnter(this, d, nodeData);
			});

		groupChild.each(function(d) {
			var nodeData = g_Instance.node(d);
			childNodeUpdate(this, d, nodeData);
		});

		groupChild.exit()
			.each(function(d) {
				d3.select(this).remove();
				console.info("remove small node: " + d);
			})
			.remove();
	},

	/**
	 * 用于节点
	 */
	_groupNode: function(g) {

		var groupSmallNode = d3.select("#rectSmallContainer").selectAll(".nodeSmallG");
		var groupSmall = groupSmallNode.data(nodes, function(d) {
			return d;
		});
		groupSmall.enter().append("circle") //enter
			.each(function(d) {
				var nodeData = g.node(d);
				smallNodeEnter(this, d, nodeData);
			});

		groupSmall.each(function(d) {
			var nodeData = g.node(d);
			smallNodeUpdate(this, d, nodeData);
		});

		groupSmall.exit()
			.each(function(d) {
				d3.select(this).remove();
				console.info("remove small node: " + d);
			})
			.remove();

		var groupNode = d3.select("#rectContainer").selectAll(".nodeG");
		var group = groupNode.data(nodes, function(d) {
			return d;
		});

		group.enter().append("g") //enter
			.each(function(d) {
				var nodeData = g.node(d);
				enter(this, d, nodeData);
			});

		group.each(function(d) { //update
			var nodeData = g.node(d);
			update(this, d, nodeData);
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
					.attr("onclick","_event.clickPath()")
					.attr("marker-end", "url(#arrow)");
			});

		groupEdgesWithData.each(function(d) {
			var pro = g.edge(d);
			var points = pro.points;
			var linegroup = d3.select(this);
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

// Create a new directed graph---------start------------------------------------------------------
/**
 * 1.1
 * 创建画版
 */
var g = new dagre.graphlib.Graph({
	compound: true
});

var param = {
	name: "test",
	rankdir: 'TB',
	/**
	 * Dagre's nodesep param - number of pixels that
	 * separate nodes horizontally in the layout.
	 *
	 * See https://github.com/cpettitt/dagre/wiki#configuring-the-layout
	 */
	nodeSep: 30,
	/**
	 * Dagre's ranksep param - number of pixels
	 * between each rank in the layout.
	 *
	 * See https://github.com/cpettitt/dagre/wiki#configuring-the-layout
	 */
	rankSep: 30,
	/**
	 * Dagre's edgesep param - number of pixels that separate
	 * edges horizontally in the layout.
	 */
	edgeSep: 5
};

// Set an object for the graph label
g.setGraph(param);

// Default to assigning a new object as a label for each new edge.
g.setDefaultEdgeLabel(function() {
	return {};
});
//Create a new directed graph---------end------------------------------------------------------


//Create a new directed dataSet---------start------------------------------------------------------
var init_child_svg = function() {
		var g_child = new dagre.graphlib.Graph({
			compound: true
		});

		var param_child = {
			name: "test",
			rankdir: 'LR',
			nodeSep: 5,
			rankSep: 5,
			edgeSep: 5
		};

		// Set an object for the graph label
		g_child.setGraph(param_child);

		// Default to assigning a new object as a label for each new edge.
		g_child.setDefaultEdgeLabel(function() {
			return {};
		});
		return g_child;
	}
	//Create a new directed dataSet---------end--------------------------------------------------------

//Create a new directed dataSet---------start------------------------------------------------------
var init_data_svg = function() {

	}
	//Create a new directed dataSet---------end--------------------------------------------------------
var interpolate = d3.svg.line()
	.interpolate('basis')
	.x(function(d) {
		return d.x;
	})
	.y(function(d) {
		return d.y;
	});

var glist = [];

var childsvg = {
	initNewInstance: function(gId) {
		var obj = init_child_svg();
		glist.push({
			k: gId,
			v: obj
		});
		return obj;
	},
	find: function(gId) {
		var obj = null;
		each(glist, function(i, o) {
			if(this.k.localeCompare(gId) == 0) {
				obj = this.v;
				return false;
			}
			return true;
		});
		return obj;
	},
	delete: function() {
		each(glist, function(i, o) {
			if(this.k.localeCompare(gId) == 0) {
				glist.splice(i, 1);
				return false;
			}
			return true;
		});
	},
	clearEdge: function(gId) {
		var gInstances = find(gId);
		var edges = gInstances.edges();
		$.each(edges, function(i, d) {
			g.removeEdge(d.v, d.w);
		});

		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				if(this.inLets.length > 0) {
					if(this.inLets.length == 1) {

					} else {
						for(var i = 0; i < this.inLets.length - 1; i++) {

						}
					}
				}
				if(this.outlets.length > 0) {

				}
				return false;
			}
			return true;
		});
	}
}