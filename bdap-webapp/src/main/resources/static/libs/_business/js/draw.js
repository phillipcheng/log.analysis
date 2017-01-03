var _draw = {
	_selectAllRemoveClassName: function(selectClassName, removeClassName, exceptId, exceptClassName) {
		d3.selectAll("." + selectClassName)
			.each(function() {
				var o = d3.select(this);
				if(o.attr("id").toString().localeCompare(exceptId) != 0) {
					var tempClassName = o.attr("class");
					tempClassName = tempClassName.replace(" " + removeClassName, "");
					o.attr("class", tempClassName);
				} else {
					o.attr("class", exceptClassName);
				}
			});
		d3.select("#" + exceptId).attr("class", exceptClassName);
	},
	_drawInputData: function(gId) {
		console.log("----------_drawInputData------------");
		console.log(arguments);
		d3.select("#" + gId).selectAll(".inPath").remove();
		var nodeData = g.node(gId);
		var gmain = d3.select("#" + gId);
		each(result.nodes, function() {
			if(this.id.toString().localeCompare(gId) == 0) {
				console.log("this:", this);
				each(this.inLets, function(i, o) {
					console.log("o", o);
					if(o.show) {
						if(o.state.localeCompare("show") == 0) {
							var tempg = gmain.append("g").attr("transform", "translate(" + (40 * i) + ",0)scale(1,1)")
								.attr("class", "inPath").attr("self", "showInData").attr("G", gId).attr("arge", o.id)
								.attr("onclick", "_event.addIn_click()");
							var tempcircle = tempg.append("circle").attr("G", gId).attr("r", "5")
								.attr("self", "showInData").attr("arge", o.id);
							var temppath = tempg.append("path").attr("self", "showInData")
								.attr("d", "M-3,0L3,0M0,3L0,-3").attr("G", gId).attr("arge", o.id);
						} else if(o.state.localeCompare("hide") == 0) {
							var tempg = gmain.append("g").attr("transform", "translate(" + (40 * i) + ",0)scale(1,1)")
								.attr("class", "inPath").attr("self", "hideInData").attr("G", gId).attr("arge", o.id)
								.attr("onclick", "_event.addIn_click()");
							var tempcircle = tempg.append("circle").attr("G", gId).attr("r", "5")
								.attr("self", "hideInData").attr("arge", o.id);
							var temppath = tempg.append("path").attr("self", "hideInData")
								.attr("d", "M-3,0L3,0").attr("G", gId).attr("arge", o.id);
						}
					}
					return true;
				});
				return false;
			}
			return true;
		});
		console.log(result);
	},
	_drawOutputData: function(gId) {
		d3.select("#" + gId).selectAll(".outPath").remove();
		var nodeData = g.node(gId);
		var gmain = d3.select("#" + gId);
		each(result.nodes, function() {
			if(this.id.toString().localeCompare(gId) == 0) {
				each(this.outlets, function(i, o) {
					if(o.show) {
						if(o.state.localeCompare("show") == 0) {
							var tempg = gmain.append("g").attr("transform", "translate(" + (40 * i) + "," + nodeData.height + ")scale(1,1)")
								.attr("class", "outPath").attr("self", "showInData").attr("G", gId).attr("arge", o.id)
								.attr("onclick", "_event.addOut_click()");
							var tempcircle = tempg.append("circle").attr("G", gId).attr("r", "5")
								.attr("self", "showInData").attr("arge", o.id);
							var temppath = tempg.append("path").attr("self", "showInData")
								.attr("d", "M-3,0L3,0M0,3L0,-3").attr("G", gId).attr("arge", o.id);
						} else if(o.state.localeCompare("hide") == 0) {
							var tempg = gmain.append("g").attr("transform", "translate(" + (40 * i) + "," + nodeData.height + ")scale(1,1)")
								.attr("class", "outPath").attr("self", "hideInData").attr("G", gId).attr("arge", o.id)
								.attr("onclick", "_event.addOut_click()");
							var tempcircle = tempg.append("circle").attr("G", gId).attr("r", "5")
								.attr("self", "hideInData").attr("arge", o.id);
							var temppath = tempg.append("path").attr("self", "hideInData")
								.attr("d", "M-3,0L3,0").attr("G", gId).attr("arge", o.id);
						}
					}
					return true;
				});
				return false;
			}
			return true;
		});
		console.log(result);
	},
	_drawDataInPutAndOutPut: function(gId) {
		var nodeData = g.node(gId);
		var hadInput = false;
		var aryInput = [];
		var hadOutput = false;
		var aryOutput = [];
		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				each(this.inLets, function(i, o) {
					if(o.state.toString().localeCompare("hide") == 0) {
						hadInput = true;
						aryInput.push(i);
					}
					return true;
				});
				each(this.outlets, function(i, o) {
					if(o.state.toString().localeCompare("hide") == 0) {
						hadOutput = true;
						aryOutput.push(i);
					}
					return true;
				});
				return false;
			}
			return true;
		});

		//展开内容
		if(hadInput) {
			if(aryInput.length == 1) {
				nodeData.height = nodeData.height + _node_data_height;
			}
		}
		if(hadOutput) {
			if(aryOutput.length == 1) {
				nodeData.height = nodeData.height + _node_data_height;
			}
		}

		if(hadInput || hadInput) {
			nodeData.width = _node_max_width;
		}

		_build._build();

		var g_child_instance = childsvg.find(gId);
		each(aryInput, function() {
			var inputObj = g_child_instance.node(gId + "_InData_" + this);
			inputObj.width = _node_data_width;
			inputObj.height = _node_data_height;
			inputObj.rect.width = _node_data_width;
			inputObj.rect.height = _node_data_height;
			_build._build(gId, g_child_instance);
			return true;
		});

		each(aryOutput, function() {
			var inputObj = g_child_instance.node(gId + "_OutData_" + this);
			inputObj.width = _node_data_width;
			inputObj.height = _node_data_height;
			inputObj.rect.width = _node_data_width;
			inputObj.rect.height = _node_data_height;
			_build._build(gId, g_child_instance);
			return true;
		});
		
	},
	_drawProperty: function(gId, obj) {
		console.log("-------------_drawProperty-------------------");
		console.log(arguments);
		var nodeData = g.node(gId);
		console.log("-------------_drawProperty------------------");
		console.log("arguments", arguments);
		$("#rightupcssbody").html("");

		d3.select("#divrightup").style({
			"display": "block"
		});

		var notProperty = "@class,id";
		var _index = 0;
		$.each(obj, function(k, v) {
			if(typeof v == 'string') {
				if(notProperty.indexOf(k) == -1) {
					_index++;
					if(k.localeCompare("name") == 0) {
						$("#rightupcssheaderstring").html(v);
					}
					var divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
					divobj.append("strong").text(k + ":");
					divobj.append("input").attr("type", "text").attr("value", v).attr("placeholder", "...")
						.attr("onkeyup", "changeProperty('" + gId + "','" + k + "')");
				}
			}
		});

		//画 property 哈哈
		var g_child_Instance = childsvg.find(gId);
		if(g_child_Instance == null) {
			g_child_Instance = childsvg.initNewInstance(gId);
			console.log("checked", g_child_Instance);
		}

		nodeData.width = _node_max_width;
		nodeData.height = nodeData.height + (_index * 20);
		nodeData.pro.transform = "translate(" + (_node_max_width - 10) + ",10)scale(1,1)";
		_build._build();

		var propertyData = g_child_Instance.node(gId + "_property");
		console.log("propertyData", propertyData);
		propertyData.width = _node_max_property_width;
		propertyData.rect.width = _node_max_property_width;
		propertyData.rect.height = _index * 20;
		if(document.getElementById(gId + "_property").getElementsByTagName("text") &&
			document.getElementById(gId + "_property").getElementsByTagName("text").length > 0) {

		} else {
			var property_no_txt = "@class,id";
			var txt_index = 0;
			each(result.nodes, function() {
				if(this.id.localeCompare(gId) == 0) {
					$.each(this, function(k, v) {
						var temp_k = k.toString().replace(".", "");
						if(typeof v == 'string') {
							if(property_no_txt.indexOf(k) == -1) {
								txt_index++;
								d3.select("#" + gId + "_property")
									.append("text").attr("id", gId + "_property_" + temp_k)
									.attr("x", 0).attr("y", (txt_index * 20) - 5).text(k + ":" + v);
							}
						}
					});
					return false;
				}
				return true;
			});
		}
		_build._build(gId, g_child_Instance);

	},
	_drawClearProperty: function() {
		$("#rightupcssbody").html("");
		d3.select("#divrightup").style({
			"display": "none"
		});
	}
}

var dataSetList = [];

var addLeftDiv = function(keys) {
	dataSetList.unshift({
		k: keys,
		show: false,
		v: {
			name: 'data',
			location: '',
			dataFormat: '',
			recordType: '',
			instance: 'false'
		}
	});

	result.data.push({
		id: keys,
		name: 'data',
		location: '',
		dataFormat: '',
		recordType: '',
		instance: 'false'
	})

	display();
}

/**
 * 
 * @param {Object} keys
 */
var deleteLeftDiv = function(keys) {
	var e = window.event || arguments.callee.caller.arguments[0];
	each(dataSetList, function(i, o) {
		if(this.k.localeCompare(keys) == 0) {
			dataSetList.splice(i, 1);
			return false;
		}
		return true;
	});

	each(result.data, function(i, o) {
		if(o.id.localeCompare(keys) == 0) {
			result.data.splice(i, 1);
			return false;
		}
		return true;
	});

	console.log(result.data);

	display();
}

var display = function() {
	$("#accordion").html("");
	each(dataSetList, function(i, o) {
		var txt = "<div class='panel panel-default'>";
		txt += "<div class='panel-heading' onclick='changeDisplay(\"" + o.k + "\")'><table style='width:100%'><tr><td style='text-align:left;width:90%'><a id='datasetheader_" + o.k + "' data-parent='#accordion' href='javascript:;'>" + o.v.name + "</a></td><td style='text-align:right;'><a href='javascript: ;' class='imgclose' onclick='deleteLeftDiv(\"" + o.k + "\")'>&nbsp;&nbsp;&nbsp;&nbsp;</a></td></tr></table></div>";
		if(o.show) {
			txt += "<div id='collapse_" + o.k + "' class='panel-collapse collapse in'>";
		} else {
			txt += "<div id='collapse_" + o.k + "' class='panel-collapse collapse'>";
		}
		txt += "<div class='panel-body'>";
		txt += "<strong>name:</strong><input args='name' style='width: 100%;' type='text'  placeholder='...' onkeyup='changeDataSetProeroty(\"" + o.k + "\")' value='" + o.v.name + "'>";
		txt += "<strong>location:</strong><input args='location' style='width: 100%;' type='text'  placeholder='...' onkeyup='changeDataSetProeroty(\"" + o.k + "\")' value='" + o.v.location + "'>";
		txt += "<strong>dataFormat:</strong><select args='dataFormat' style='width: 100%;' onchange='changeDataSetProeroty(\"" + o.k + "\")'>";
		txt += "<option value='Line'>Line</option><option value='XML'>XML</option><option value='Text'>Text</option><option value='Binary'>Binary</option>";
		txt += "<option value='Section'>Section</option><option value='Mixed'>Mixed</option><option value='FileName'>FileName</option></select>";
		txt += "<strong>recordType:</strong><select args='recordType' style='width: 100%;' onchange='changeDataSetProeroty(\"" + o.k + "\")'>";
		txt += "<option value='Path'>Path</option><option value='KeyPath'>KeyPath</option><option value='Value'>Value</option><option value='KeyValue'>KeyValue</option></select>";
		txt += "<strong>instance:</strong><input args='instance' type='checkbox' onchange='changeDataSetProeroty(\"" + o.k + "\")'>";
		txt += "</div></div></div>";
		var content = $("#accordion").html();
		content += txt;
		$("#accordion").html(content);
		return true;
	});
}

/**
 * 
 * @param {Object} objId
 */
var changeDisplay = function(objId) {
	if(document.getElementById("collapse_" + objId)) {
		if(document.getElementById("collapse_" + objId).className.toString().indexOf("in") > -1) {
			d3.select("#accordion").select("#collapse_" + objId).attr("class", "panel-collapse collapse");
			each(dataSetList, function() {
				if(this.k.localeCompare(objId) == 0) {
					this.show = false;
					return false;
				}
				return true;
			});
		} else {
			d3.select("#accordion").select("#collapse_" + objId).attr("class", "panel-collapse collapse in");
			each(dataSetList, function() {
				if(this.k.localeCompare(objId) == 0) {
					this.show = true;
					return false;
				}
				return true;
			});
		}
	}
	display();
}

/**
 * 改变dataSet属性变化
 */
var changeDataSetProeroty = function(gId) {
	var txt_val = "";
	var e = window.event || arguments.callee.caller.arguments[0];
	var o = getEventSources(e);
	var tempProperty = o.getAttribute("args").toString();
	if(tempProperty.localeCompare("instance") == 0) {
		if(o.checked) {
			txt_val = "true";
		} else {
			txt_val = "false";
		}
	} else {
		txt_val = o.value;
	}
	each(dataSetList, function() {
		if(this.k.toString().localeCompare(gId) == 0) {
			this.v[tempProperty] = txt_val;
			return false;
		} else {
			return true;
		}
	});
	if(tempProperty.localeCompare("name") == 0) {
		document.getElementById("datasetheader_" + gId).innerHTML = txt_val;
	}

	each(result.data, function(i, o) {
		if(o.id.localeCompare(gId) == 0) {
			o[tempProperty] = txt_val;
			return false;
		}
		return true;
	});
	console.log(result);
}
var changeProperty = function(gId, propertyName) {
	console.log("----------changeProperty------------");
	console.log("arguments", arguments);
	var e = window.event || arguments.callee.caller.arguments[0];
	var o = getEventSources(e);
	if(propertyName.localeCompare("name") == 0) {
		$("#rightupcssheaderstring").html(o.value);
		d3.select("#" + gId).select("text").text(o.value);
	}
	each(result.nodes, function() {
		if(this.id.localeCompare(gId) == 0) {
			this[propertyName] = o.value;
			var tempPropertyName = propertyName.replace(".", "");
			console.log(d3.select("#" + gId + "_property_" + tempPropertyName));
			d3.select("#" + gId + "_property").select("#" + gId + "_property_" + tempPropertyName).text(propertyName + ":" + o.value);
			return false;
		}
		return true;
	});
}