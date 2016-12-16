var app = {
	action: function(jsonObj) {
		nodeIndex++;
		var temp_g = "g_" + (new Date().getTime() + nodeIndex);
		var obj = {
			label: jsonObj.label,
			width: 200,
			height: 50,
			runstate: 'play',
			action: 'node',
			zoom: 'normal'
		};
		dataList.push({
			k:temp_g,
			v:{
				name:'',
				location:'',
				dataFormat:'',
				recordType:'',
				instance:'true'
			}
		});
		$.each(remoteActionObj, function(k, v) {
			if(k.toString().indexOf(jsonObj.label)>-1) {
				var ary = new Array();
				ary.push({
					k: '@class',
					v: ''
				});
				ary.push({
					k: 'name',
					v: ''
				});
				ary.push({
					k: 'inlets',
					v: ''
				});
				ary.push({
					k: 'outlets',
					v: ''
				});
				ary.push({
					k: 'input.format',
					v: ''
				});
				each(v, function(i, o) {
					ary.push({
						k: v[i],
						v: ''
					});
					return true;
				})
				propertyList.push({
					key: temp_g,
					type: k,
					value: ary
				});
				console.log("propertyList", propertyList);
				setSelfNode(temp_g, obj);
				_base._build();
				return false;
			} else {
				return true;
			}
		});
	},
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
		};
		d3.json(_HTTP_LOAD_PROPERTY, function(data) {
			propertyList.push({
				key: temp_g,
				value: data
			});
			setSelfNode(temp_g, obj);
			_base._build();
		});
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
	d3.select("#" + d).select(".nodePropertyG").remove();
}

/**
 * 打开加载属性列表
 * @param {Object} d
 * @param {Object} nodeData
 */
var loadProperty = function(d, nodeData) {
		console.log(document.getElementById(d).getElementsByTagName("text")[0].innerHTML);
		d3.select("#" + d).select(".nodePropertyG").remove();
		d3.select(".rightupcssbody").selectAll(".sublistgroup").remove();
		d3.select("#divrightup").select(".rightupcssheader").select("strong").text(document.getElementById(d).getElementsByTagName("text")[0].innerHTML);
		each(propertyList, function(i, v) {
			if(v.key.toString().localeCompare(d.toString()) == 0) {
				console.log("propertylist", v.value);
				d3.select("#" + d).append("g")
					.attr("id", "propertylist_" + d)
					.attr("class", "nodePropertyG")
					.append("rect")
					.attr("width", nodeData.width - 20)
					.attr("height", nodeData.height - 50)
					.attr("x", 10)
					.attr("y", 25);
					var _index = 0;
					var svgrect = d3.select("#propertylist_" + d);
				each(v.value, function(index, obj) {
					if(this.k.toString().localeCompare("inlets") == 0 || this.k.toString().localeCompare("outlets") == 0) {
						
					} else {
						_index++;
						var theselect = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
						theselect.append("strong").text(this.k + ":");
						theselect.append("input").attr("type", "text")
							.attr("value", this.v).attr("placeholder", "...")
							.attr("onkeyup", "changeProperty('" + d + "','" + this.k + "','propertylist_" + d + "_" + index + "')");
						svgrect	.append("text")
							.attr("id", "propertylist_" + d + "_" + index)
							.attr("x", 15)
							.attr("y", 25 + (_index * 18))
							.text(this.k + ":" + this.v);		
					}
					return true;
				});
				return false
			} else {
				return true;
			}
		});

		d3.select("#divrightup").style({
			"display": "block"
		});

	}

/**
 * 属性改变事件
 * @param {Object} d
 * @param {Object} keys
 * @param {Object} proId
 */
var changeProperty = function(d, keys, proId) {
	d3.select("#" + proId).text(keys + ":" + getEventSources().value);
	each(propertyList, function(i, o) {
		if(o.key.toString().localeCompare(d) == 0) {
			each(o.value,function(){
				if(this.k.toString().localeCompare(keys.toString())==0){
					this.v = getEventSources().value;
				}
				return true;
			});
			return false;
		} else {
			return true;
		}
	});
}

/**
 * 
 * @param {Object} d
 * @param {Object} keys
 */
var changeDataSet = function(d,keys){
		each(dataList, function(i, o) {
		if(o.key.toString().localeCompare(d) == 0) {
			each(o.value,function(){
				if(this.k.toString().localeCompare(keys.toString())==0){
					this.v = getEventSources().value;
				}
				return true;
			});
			return false;
		} else {
			return true;
		}
	});
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
				}
				break;
			case "HideProperty":
				{
					countProperty = 0;
					clearProperty(o.getAttribute("G").toString());
					this.HideProperty(o.getAttribute("G").toString());
					setTimeout(function() {
						console.log("----------------setTimeout 1000---------------------");
						if(countProperty == 0) {
							d3.select("#divrightup").style({
								"display": "none"
							});
						}
					}, 1000);
				}
				break;
			case "ShowChildren":
				{
					this.ShowChildren(o.getAttribute("G").toString());
					d3.select("#" + o.getAttribute("G").toString()).select("text").style({
						"opacity": 0
					});
				}
				break;
			case "HideChildren":
				{
					this.HideChildren(o.getAttribute("G").toString());
					d3.select("#" + o.getAttribute("G").toString()).select("text").style({
						"opacity": 1
					});
				}
				break;
			case "Log":
				{
					openLogWin();
				}
				break;
			case "OutPath":
				{
					openDatasetWin();
				}
				break;
			case "InPath":
				{
					openDatasetWin();
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
			width: (nodeData.width || 100) * 1.5,
			height: (nodeData.height || 50) * 8,
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
			width:200,
			height: 50,
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
		g.setEdge("childone", "childtwo");

		g.setParent("childone", keys);
		g.setParent("childtwo", keys);

		//++++++++++++++++++++++++++++++++end+++++++++++++++++++++++++++++		

		_base._build();
	},
	HideChildren: function(keys) {
		var nodeData = g.node(keys);
		var obj = {
			label: nodeData.label || "",
			width: 100,
			height: 50,
			runstate: nodeData.runstate || 'play',
			action: 'group',
			zoom: 'normal'
		};
		console.log(obj);
		g.removeNode("childone");
		g.removeNode("childtwo");
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
	_svg: function(o) {
		booleaniszoom = false;
		clearTempLine();
	},
	_rect: function(o) {
		//
		if(o.getAttribute("G")) {
			if(d3.select("#" + o.getAttribute("G").toString()).select(".maxNodeG")[0][0]) {
				var tempd = o.getAttribute("G").toString();
				var tempNodeData = g.node(o.getAttribute("G").toString());
				loadProperty(tempd, tempNodeData);
			}
		}
		if(templine.firstId.length == 0) {
			//进行选中的操作
			try {
				if(o.getAttribute("class").toString().localeCompare("nodeRect") == 0) {
					o.setAttribute("class", "nodeRectSelected");
				} else {
					o.setAttribute("class", "nodeRect");
				}
			} catch(err) {

			} finally {
				document.getElementById("svg").style.cursor = "default";
			}
		} else {
			//画第二个点,进行连线的操作
//			if(o.getAttribute("G")) {
//				templine.endId = o.getAttribute("G").toString();
//				console.log(templine);
//				g.setEdge(templine.firstId, templine.endId);
//				_base._build();
//				selectionId = "";
//				pathLists.push({
//					from: templine.firstId,
//					to: templine.endId
//				});
//				console.log("propertyList-start", propertyList);
//				each(propertyList, function(i, o) {
//					if(o.key.toString().localeCompare(templine.firstId) == 0) {
//						//开始点
//						this.value.outlets++;
//					} else if(o.key.toString().localeCompare(templine.endId) == 0) {
//						//结束站
//						this.value.inlets++;
//					}
//					return true;
//				});
//				console.log("propertyList-end", propertyList);
//				clearTempLine();
//				document.getElementById("svg").style.cursor = "default";
//			}
		}
	},
	_path: function(o) {
		console.log("_path", o);
		if(o.getAttribute("self")) {

		} else {
			d3.select("#" + o.getAttribute("G")).attr("class", "edgeSelected");
			o.setAttribute("marker-end", "url(#arrowSelected)");
		}

	}
}