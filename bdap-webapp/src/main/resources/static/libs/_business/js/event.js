var g_mouse_down = "";
var g_mouse_up = "";

var _event = {
	svg_onmousedown: function() {
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		if(o.tagName.localeCompare("svg") == 0) {
			booleaniszoom = true;
			clearTempLine();
		}
	},
	svg_onmouseup: function() {
		clearTempLine();
		booleaniszoom = false;
	},
	line_onmousedown: function() {
		booleaniszoom = false;
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		e.stopPropagation();
		console.log("line_onmousedown:", o);
		g_mouse_down = "";
		g_mouse_up = "";

		g_mouse_down = o.getAttribute("G");
		var nodeData = g.node(g_mouse_down);

		templine.firstId = g_mouse_down;
		templine.firstPoint = nodeData.x + "," + nodeData.y;

	},
	line_onmouseup: function() {
		booleaniszoom = false;
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		e.stopPropagation();
		console.log("line_onmouseup:", o);
		g_mouse_up = o.getAttribute("G");
		if(g_mouse_down.localeCompare(g_mouse_up) == 0) {
			this.clickedRect();
			clearTempLine();
			g_mouse_down = "";
			g_mouse_up = "";
			return false;
		} else {
			g.setEdge(g_mouse_down, g_mouse_up);
			g_mouse_down = "";
			g_mouse_up = "";
			_build._build();
		}
		clearTempLine();
	},
	addIn_click: function() {
		console.log("------addIn_click-------");
		var e = window.event || arguments.callee.caller.arguments[0];
		e.stopPropagation();
		var o = getEventSources(e);
		var gId = o.getAttribute("G");
		var objId = o.getAttribute("arge");
		console.log("addIn_click:", o);
		var args_self = o.getAttribute("self");
		if(args_self.localeCompare("showInData") == 0) {
			each(result.nodes, function() {
				if(this.id.localeCompare(gId) == 0) {
					each(this.inLets, function() {
						if(this.id.localeCompare(objId) == 0) {
							this.state = "hide";
							_draw._drawInputData(gId);
							//判断，确定宽度
							return false;
						}
						return true;
					});
					return false;
				}
				return true;
			});

		} else if(args_self.localeCompare("hideInData") == 0) {
			each(result.nodes, function() {
				if(this.id.localeCompare(gId) == 0) {
					each(this.inLets, function() {
						if(this.id.localeCompare(objId) == 0) {
							this.state = "show";
							_draw._drawInputData(gId);
							return false;
						}
						return true;
					});
					return false;
				}
				return true;
			});
		}

		_draw._drawDataInPutAndOutPut(gId);
	},
	addOut_click: function() {
		console.log("------addOut_click-------");
		var e = window.event || arguments.callee.caller.arguments[0];
		e.stopPropagation();
		var o = getEventSources(e);
		var gId = o.getAttribute("G");
		var objId = o.getAttribute("arge");
		console.log("addOut_click:", o);
		var args_self = o.getAttribute("self");
		if(args_self.localeCompare("showInData") == 0) {
			each(result.nodes, function() {
				if(this.id.localeCompare(gId) == 0) {
					each(this.outlets, function() {
						if(this.id.localeCompare(objId) == 0) {
							this.state = "hide";
							_draw._drawOutputData(gId);
							//判断，确定宽度
							return false;
						}
						return true;
					});
					return false;
				}
				return true;
			});

		} else if(args_self.localeCompare("hideInData") == 0) {
			each(result.nodes, function() {
				if(this.id.localeCompare(gId) == 0) {
					each(this.outlets, function() {
						if(this.id.localeCompare(objId) == 0) {
							this.state = "show";
							_draw._drawOutputData(gId);
							return false;
						}
						return true;
					});
					return false;
				}
				return true;
			});
		}

		_draw._drawDataInPutAndOutPut(gId);
	},
	property_click: function() {
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		e.stopPropagation();
		console.log("property_click:", o);
		var args = o.getAttribute("self");
		var gId = o.getAttribute("G");
		var nodeData = g.node(gId);
		var tempClass = nodeData.class;
		tempClass = tempClass + " nodeGSelected";
		_draw._selectAllRemoveClassName("nodeGSelected", "nodeGSelected", gId, tempClass);
		var temptxt = g.node(gId).txt.txt;
		var stateTxt = g.node(gId).state;
		var obj = {};
		if(args.localeCompare("ShowProperty") == 0) {
			nodeData.pro.self = "HidProperty";
			nodeData.pro.circle.self = "HidProperty";
			nodeData.pro.path.self = "HidProperty";
			nodeData.pro.path.d = "M-3,0L3,0";
			each(result.nodes, function() {
				if(this.id.localeCompare(gId) == 0) {
					console.log(this);
					_draw._drawProperty(gId, this);
					return false;
				}
				return true;
			});
			_build._build();
		} else {
			nodeData.pro.self = "ShowProperty";
			nodeData.pro.circle.self = "ShowProperty";
			nodeData.pro.path.self = "ShowProperty";
			nodeData.pro.path.d = "M-3,0L3,0M0,-3L0,3";
			_draw._drawClearProperty();
			_build._build();
		}
	},
	data_addIn_click: function() {
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		var gId = o.getAttribute("G");
		console.log("data_addIn_click:", o);
		each(result.nodes, function(i, o) {
			if(o.id.toString().localeCompare(gId) == 0) {
				console.log(o.inLets.length);
				each(o.inLets, function() {
					if(!this.show) {
						this.show = true;
						_draw._drawInputData(gId);
						return false;
					}
					return true;
				});

				return false;
			}
			return true;
		});
	},
	data_addOut_click: function() {
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		var gId = o.getAttribute("G");
		console.log("data_addOut_click:", o);
		each(result.nodes, function(i, o) {
			if(o.id.toString().localeCompare(gId) == 0) {
				console.log(o.outlets.length);
				each(o.outlets, function() {
					if(!this.show) {
						this.show = true;
						_draw._drawOutputData(gId);
						return false;
					}
					return true;
				});

				return false;
			}
			return true;
		});
	},
	clickedRect: function() {
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		var gId = o.getAttribute("G");
		each(result.nodes, function() {
			var tempClassName = d3.select("#" + this.id).attr("class")
			if(this.id.localeCompare(gId) == 0) {
				//nodeGSelected
				if(tempClassName.indexOf("nodeGSelected") == -1) {
					tempClassName += " nodeGSelected";
					d3.select("#" + this.id).attr("class", tempClassName);
				}
			} else {
				if(tempClassName.indexOf("nodeGSelected") > -1) {
					tempClassName = tempClassName.replace("nodeGSelected", "");
					d3.select("#" + this.id).attr("class", tempClassName);
				} else {
					tempClassName += " nodeGSelected";
					d3.select("#" + this.id).attr("class", tempClassName);
				}
			}
			return true;
		});
	},
	selectedProperty: function() {
		console.log("--------------------selectedProperty----------------------");
		console.log(arguments);
		//这里 只是 右边的，对话框，进行展示内容
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		var gId = o.getAttribute("G");
		$("#rightupcssbody").html("");
		var notProperty = "@class,id";
		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				$.each(this, function(k, v) {
					if(typeof v == 'string') {
						if(notProperty.indexOf(k) == -1) {
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
				return false;
			}
			return true;
		});
	}
}