var app = {
	start: function() {
		nodeIndex++;
		var temp_g = "g_" + (new Date().getTime() + nodeIndex);
		var obj = {
			label: "Star",
			width: 100,
			height: 50,
			runstate: 'play',
			action: 'node',
			zoom: 'normal'
		}
		setSelfNode(temp_g, obj);
		_base._build();
	},
	end: function() {
		nodeIndex++;
		var temp_g = "g_" + (new Date().getTime() + nodeIndex);
		var obj = {
			label: "End",
			width: 100,
			height: 50,
			runstate: 'play',
			action: 'node',
			zoom: 'normal'
		}
		setSelfNode(temp_g, obj);
		_base._build();
	},
	group: function() {
		nodeIndex++;
		var temp_g = "g_" + (new Date().getTime() + nodeIndex);
		var obj = {
			label: "group",
			width: 100,
			height: 50,
			runstate: 'play',
			action: 'group',
			zoom: 'normal'
		}
		setSelfNode(temp_g, obj);
		_base._build();
	}
}

/**
 * 
 */
var run = function() {
	var temp = d3.select("#outPath").attr("d");
	d3.select("#lineContainer")
		.append("path").attr("d", temp).attr("class", "edgerun").call(transition);
}

/**
 * 设置节点属性
 * @param {Object} keys
 * @param {Object} valueObj
 */
var setSelfNode = function(keys, valueObj) {
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
 * 清除一个节点下的所有属性
 * @param {Object} d
 */
var clearProperty = function(d) {		
		console.log(d);
		console.log(d3.select("#" + d));
		console.log(d3.select("#" + d).select(".nodePropertyG"));
		d3.select("#" + d).select(".nodePropertyG").remove();
}

/**
 * 打开加载属性列表
 * @param {Object} d
 * @param {Object} nodeData
 * @param {Object} propertyJSON
 */
var loadProperty = function(d, nodeData, propertyJSON) {
	d3.select("#" + d).select(".nodePropertyG").remove();
	d3.select("#divrightup").selectAll(".sublistgroup").remove();
	propertyJSON = propertyJSON || [];
	propertyJSON.push({
		k: '@class',
		v: 'start1'
	});
	propertyJSON.push({
		k: 'name',
		v: 'start2'
	});
	propertyJSON.push({
		k: 'propertyone',
		v: 'start3'
	});

	d3.select("#divrightup").style({
		'display': 'block'
	});

	d3.select("#" + d).append("g")
			.attr("id", "propertylist_" + d)
			.attr("class", "nodePropertyG")
		.append("rect")
		.attr("width", nodeData.width - 20)
		.attr("height", nodeData.height - 50)
		.attr("x", 10)
		.attr("y", 25);

	each(propertyJSON, function(i, o) {
		d3.select("#propertylist_" + d)
			.append("text")
			.attr("id", "propertylist_" + d + "_" + i)
			.attr("x", 15)
			.attr("y", 45 + (i * 18))
			.text(o.k + ":" + o.v);

		d3.select("#divrightup").append("div").attr("class", "sublistgroup");

		d3.select("#divrightup").select(".sublistgroup")
			.append("strong").text(o.k + ":");

		d3.select("#divrightup").select(".sublistgroup")
			.append("input").attr("type", "text")
			.attr("value", o.v).attr("placeholder", "class...")
			.attr("onkeyup", "changeProperty('" + o.k + "','propertylist_" + d + "_" + i + "')");
		return i;
	});
}

/**
 * 
 * @param {Object} proId
 */
var changeProperty = function(keys, proId) {
	d3.select("#" + proId).text(keys + ":" + getEventSources().value);
}

/**
 * 特别的操作效果
 */
var flow = {
	init: function(args, o) {
		switch(args) {
			case "ShowProperty":
				{
					this.ShowProperty(o.getAttribute("G").toString());
					loadProperty(o.getAttribute("G").toString(), g.node(o.getAttribute("G").toString()));
				}
				break;
			case "HideProperty":
				{
					clearProperty(o.getAttribute("G").toString());
					this.HideProperty(o.getAttribute("G").toString());
				}
				break;
			case "ShowChildren":
				{
					this.ShowChildren(o.getAttribute("G").toString());
					d3.select("#"+o.getAttribute("G").toString()).select("text").style({
						"opacity":0
					});
				}
				break;
			case "HideChildren":
				{
					this.HideChildren(o.getAttribute("G").toString());
				}
				break;
			default:
				{
					clearTempLine();
				}
				break;
		}
	},
	ShowProperty: function(keys) {
		var nodeData = g.node(keys);
		var obj = {
			label: nodeData.label || "",
			width: (nodeData.width || 100) * 4,
			height: (nodeData.height || 50) * 4,
			runstate: nodeData.runstate || 'play',
			action: nodeData.action || 'node',
			zoom: 'max'
		}
		setSelfNode(keys, obj);
		_base._build();
	},
	HideProperty: function(keys) {
		var nodeData = g.node(keys);
		var obj = {
			label: nodeData.label || "",
			width: (nodeData.width || 100) / 4,
			height: (nodeData.height || 50) / 4,
			runstate: nodeData.runstate || 'play',
			action: nodeData.action || 'node',
			zoom: 'normal'
		}
		setSelfNode(keys, obj);
		_base._build();
	},
	ShowChildren: function(keys) {
		var nodeData = g.node(keys);
		var obj = {
			label: nodeData.label || "",
			width: (nodeData.width || 100) * 4,
			height: (nodeData.height || 50) * 4,
			runstate: nodeData.runstate || 'play',
			action: 'group',
			zoom: 'max'
		};
		setSelfNode(keys, obj);
		
		//++++++++++++++++++++++++++++++++star+++++++++++++++++++++++++++++		
		g.setNode("childone", {
			label: "childone",
			width: 100,
			height: 50,
			runstate: 'play',
			action: 'node',
			zoom: 'normal'
		});
		g.setNode("childtwo", {
			label: "childtwo",
			width: 100,
			height: 50,
			runstate: 'play',
			action: 'node',
			zoom: 'normal'
		});
		g.setEdge("childone","childtwo");	
		
		g.setParent("childone", keys);
		g.setParent("childtwo", keys);		
	
		
		//++++++++++++++++++++++++++++++++end+++++++++++++++++++++++++++++		
		
		_base._build();
	},
	HideChildren: function(keys) {
		var nodeData = g.node(keys);
		var obj = {
			label: nodeData.label || "",
			width: (nodeData.width || 100) / 4,
			height: (nodeData.height || 50) / 4,
			runstate: nodeData.runstate || 'play',
			action: 'group',
			zoom: 'normal'
		};
		setSelfNode(keys, obj);
		_base._build();
	},
	Point: function(o) {
		selectionId = o.getAttribute("G").toString();
		var g_obj = document.getElementById(selectionId);
		console.log("g_obj", g_obj);
		if(templine.firstId.length == 0) {
			//还没有点击第一个点
			templine.firstId = selectionId;
			templine.firstPoint = g_obj.getAttribute("x") + "," + g_obj.getAttribute("y");
			selectionId = "";
		} else {
			//已经点击了第一个点，这个时候,确认第二个点
			if(selectionId.localeCompare(templine.firstId) != 0) {
				templine.endId = selectionId;
				pathLists.push({
					dv: templine.firstId,
					dw: templine.endId
				});
				g.setEdge(templine.firstId, templine.endId);
				_base._build();
				clearTempLine();
				selectionId = "";
			}
		}
	}
}

/**
 * 特别的操作效果
 * 针对dom的操作与改变
 */
var graph = {
	init: function(o) {
		switch(o.tagName) {
			case "rect":
				this._rect(o);
				break;
		}
	},
	_svg: function(o) {
		booleaniszoom = false;
		clearTempLine();
	},
	_rect: function(o) {
		if(templine.firstId.length == 0) {
			//进行选中的操作
			if(o.getAttribute("class").toString().localeCompare("nodeRect") == 0) {
				o.setAttribute("class", "nodeRectSelected");
			} else {
				o.setAttribute("class", "nodeRect");
			}
		} else {
			//画第二个点,进行连线的操作
			if(o.getAttribute("G")) {
				templine.endId = o.getAttribute("G").toString();
				console.log(templine);
				g.setEdge(templine.firstId, templine.endId);
				_base._build();
				selectionId = "";
				clearTempLine();
			}
		}
	}
}