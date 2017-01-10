var g_mouse_down = "";
var g_mouse_up = "";

var _event = {
	up_flow_name: function() {
		var e = window.event || arguments.callee.caller.arguments[0];
		e.stopPropagation();
		var o = getEventSources(e);
		d3.select("#d3contextmenu").style({
			display: "block",
			left: e.clientX + "px",
			top: e.clientY + "px"
		});
		d3.select("#d3contextmenu")
			.append("input").on("blur", function() {
				d3.select("#d3contextmenu").select("input").remove();
				result.name = this.value;
				result.wfName = this.value;
				d3.select("#a_title").text(result.name);
				d3.select("#d3contextmenu").style({
					display: "none"
				});
			});
	},
	svg_onmousedown: function() {
		var e = window.event || arguments.callee.caller.arguments[0];
		e.stopPropagation();
		var o = getEventSources(e);
		if(o.tagName.localeCompare("svg") == 0) {
			booleaniszoom = true;
			clearTempLine();
		}
		d3.select("#d3contextmenu").style({
			display: "none"
		});
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
		g_mouse_up = o.getAttribute("G");
		if(g_mouse_down.localeCompare(g_mouse_up) == 0) {
			this.clickedRect();
			clearTempLine();
			g_mouse_down = "";
			g_mouse_up = "";
			return false;
		} else {
			g.setEdge(g_mouse_down, g_mouse_up);
			result.links.push({
				fromNodeName: g_mouse_down,
				toNodeName: g_mouse_up,
				linkType: "success"
			})
			g_mouse_down = "";
			g_mouse_up = "";
			_build._build();
		}
		clearTempLine();
	},
	addIn_click: function() {
		var e = window.event || arguments.callee.caller.arguments[0];
		e.stopPropagation();
		var o = getEventSources(e);
		var gId = o.getAttribute("G");
		var objId = o.getAttribute("arge");
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
			$("#rightupcssbody").html("");
			d3.select("#divrightup").style({
				display: "none"
			});
		}

		_draw._drawDataInPutAndPropertyAndOutPut(gId);
	},
	addOut_click: function() {
		var e = window.event || arguments.callee.caller.arguments[0];
		e.stopPropagation();
		var o = getEventSources(e);
		var gId = o.getAttribute("G");
		var objId = o.getAttribute("arge");
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
			$("#rightupcssbody").html("");
			d3.select("#divrightup").style({
				display: "none"
			});
		}
		_draw._drawDataInPutAndPropertyAndOutPut(gId);
	},
	property_click: function() {
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		e.stopPropagation();
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
					_draw._drawPropertyLeftDiv(gId, this);
					return false;
				}
				return true;
			});
			_build._build();
		} else {
			_draw._drawClearProperty(gId);
		}
	},
	data_addIn_click: function() {
//		var e;
//		var o;
//		if(isEmpty(addInletButtonThis)) {
//			e = window.event || arguments.callee.caller.arguments[0];
//			o = getEventSources(e);
//		} else {
//			o = addInletButtonThis;
//		}
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		e.stopPropagation();

		var gId = o.getAttribute("G");
		each(result.nodes, function(i, o) {
			if(o.id.toString().localeCompare(gId) == 0) {
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
//		var e;
//		var o;
//		if(isEmpty(addInletButtonThis)) {
//			e = window.event || arguments.callee.caller.arguments[0];
//			o = getEventSources(e);
//		} else {
//			o = addInletButtonThis;
//		}
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		e.stopPropagation();

		var gId = o.getAttribute("G");
		each(result.nodes, function(i, o) {
			if(o.id.toString().localeCompare(gId) == 0) {
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
		e.stopPropagation();
		var o = getEventSources(e);
		var gId = o.getAttribute("G");
		each(result.nodes, function() {
			if(!this.inLets) {
				d3.select("#" + this.id).attr("class", "nodeG start");
			} else if(!this.outlets) {
				d3.select("#" + this.id).attr("class", "nodeG end");
			} else {
				d3.select("#" + this.id).attr("class", "nodeG action");
			}
			return true;
		});

		var temp = d3.select("#" + gId).attr("class");
		d3.select("#" + gId).attr("class", temp + " nodeGSelected");

		if(e.button == 2) {
			//右键点击样式
			d3.select("#d3contextmenu").select("ul").remove();
			d3.select("#d3contextmenu").style({
				display: "block",
				left: e.clientX + "px",
				top: e.clientY + "px"
			});

			d3.select("#d3contextmenu").append("ul").append("li").text("delete the node?")
				.on("click", function() {
					console.log("click");
					d3.select("#d3contextmenu").style({
						display: "none"
					});
					g.removeNode(gId);
					each(result.nodes, function(i, o) {
						if(this.id.localeCompare(gId) == 0) {
							result.nodes.splice(i, 1);
							return false;
						}
						return true;
					});
					each(result.links, function(i, o) {
						if(this.fromNodeName.localeCompare(gId) == 0 || this.toNodeName.localeCompare(gId) == 0) {
							result.links.splice(i, 1);
							return false;
						}
						return true;
					});
					_build._build();
				});
		}
	},
	clickPath: function() {
		console.log("-----------clickPath-------------");
		var e = window.event || arguments.callee.caller.arguments[0];
		e.stopPropagation();
		var o = getEventSources(e);
		d3.select("#d3contextmenu").select("ul").remove();
		d3.select("#d3contextmenu").style({
			display: "block",
			left: e.clientX + "px",
			top: e.clientY + "px"
		});

		d3.select("#d3contextmenu").append("ul").append("li").text("delete the Path?")
			.on("click", function() {
				var temp = o.id.split("A");
				g.removeEdge(temp[1],temp[2]);
				each(result.links,function(i,o){
					if(this.fromNodeName.localeCompare(temp[1])==0&&this.toNodeName.localeCompare(temp[2])==0){
						result.links.splice(i,1);
						return false;
					}
					return true;
				});
				d3.select("#d3contextmenu").style({
					display: "none"
				});
				_build._build();
			});
	},
	selectedData: function(txtId, gId) {
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		e.stopPropagation();
		d3.select("#divrightup").style({
			display: "block"
		});
		$("#rightupcssbody").html("");

		var tempValue = "";
		var tempTxt = "";
		var tempIndex = txtId.substring(txtId.length - 1);

		each(result.nodes, function(i, o) {
			if(this.id.localeCompare(gId) == 0) {
				var find = false;
				var divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
				divobj.append("strong").text("name:");
				if(txtId.indexOf("InData") > -1) {
					d3.select("#rightupcssheaderstring").text(o.inLets[tempIndex].name);
					divobj.append("input").attr("type", "text").attr("value", o.inLets[tempIndex].name)
						.attr("placeholder", "...")
						.attr("onkeyup", "changeData('" + txtId + "','name','" + gId + "')")
				} else {
					d3.select("#rightupcssheaderstring").text(o.outlets[tempIndex].name);
					divobj.append("input").attr("type", "text").attr("value", o.outlets[tempIndex].name)
						.attr("placeholder", "...")
						.attr("onkeyup", "changeData('" + txtId + "','name','" + gId + "')")
				}
				divobj.append("strong").text("dataName:");

				var divobjselect = divobj.append("select");
				divobjselect.attr("onchange", "changeData('" + txtId + "','dataName','" + gId + "')");
				each(dataSetList, function() {
					if(txtId.indexOf("InData") > -1) {
						if(this.k.localeCompare(o.inLets[tempIndex].dataName) == 0) {
							divobjselect.append("option").attr("selected", "selected").attr("value", this.k).text(this.v.name);
							find = true;
						} else {
							divobjselect.append("option").attr("value", this.k).text(this.v.name);
						}
					} else {
						if(this.k.localeCompare(o.outlets[tempIndex].dataName) == 0) {
							divobjselect.append("option").attr("selected", "selected").attr("value", this.k).text(this.v.name);
							find = true;
						} else {
							divobjselect.append("option").attr("value", this.k).text(this.v.name);
						}
					}
					return true;
				});
				if(!find) {
					divobjselect[0][0].selectedIndex = -1;
				}
				return false;
			}
			return true;
		});
	},
	selectedProperty: function() {
		//这里 只是 右边的，对话框，进行展示内容
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		e.stopPropagation();
		var gId = o.getAttribute("G");
		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				_draw._drawPropertyLeftDiv(gId, this);
				return false;
			}
			return true;
		});
		//		$("#rightupcssbody").html("");
		//		var notProperty = "@class,id";
		//		each(result.nodes, function() {
		//			if(this.id.localeCompare(gId) == 0) {
		//				$.each(this, function(k, v) {
		//					if(typeof v == 'string') {
		//						if(notProperty.indexOf(k) == -1) {
		//							if(k.localeCompare("name") == 0) {
		//								$("#rightupcssheaderstring").html(v);
		//							}
		//							var divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
		//							divobj.append("strong").text(k + ":");
		//							divobj.append("input").attr("type", "text").attr("value", v).attr("placeholder", "...")
		//								.attr("onkeyup", "changeProperty('" + gId + "','" + k + "')");
		//						}
		//					}
		//				});
		//				return false;
		//			}
		//			return true;
		//		});
	}
}