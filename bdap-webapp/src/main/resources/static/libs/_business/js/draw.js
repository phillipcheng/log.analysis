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
		d3.select("#" + gId).selectAll(".inPath").remove();
		var nodeData = g.node(gId);
		var gmain = d3.select("#" + gId);
		each(result.nodes, function() {
			if(this.id.toString().localeCompare(gId) == 0) {
				each(this.inLets, function(i, o) {
					if(o.show) {
						if(o.state.localeCompare("show") == 0) {
							var tempg = gmain.append("g").attr("transform", "translate(" + (40 * i) + ",0)scale(1,1)")
								.attr("id", o.id + "_point")
								.attr("class", "inPath").attr("self", "showInData").attr("G", gId).attr("arge", o.id)
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y - nodeData.height / 2))
								.attr("onclick", "_event.addIn_click()");
							var tempcircle = tempg.append("circle").attr("G", gId).attr("r", "5")
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y - nodeData.height / 2))
								.attr("self", "showInData").attr("arge", o.id);
							var temppath = tempg.append("path").attr("self", "showInData")
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y - nodeData.height / 2))
								.attr("d", "M-3,0L3,0M0,3L0,-3").attr("G", gId).attr("arge", o.id);
						} else if(o.state.localeCompare("hide") == 0) {
							var tempg = gmain.append("g").attr("transform", "translate(" + (40 * i) + ",0)scale(1,1)")
								.attr("id", o.id + "_point")
								.attr("class", "inPath").attr("self", "hideInData").attr("G", gId).attr("arge", o.id)
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y - nodeData.height / 2))
								.attr("onclick", "_event.addIn_click()");
							var tempcircle = tempg.append("circle").attr("G", gId).attr("r", "5")
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y - nodeData.height / 2))
								.attr("self", "hideInData").attr("arge", o.id);
							var temppath = tempg.append("path").attr("self", "hideInData")
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y - nodeData.height / 2))
								.attr("d", "M-3,0L3,0").attr("G", gId).attr("arge", o.id);
						}
					}
					return true;
				});
				return false;
			}
			return true;
		});
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
								.attr("id", o.id + "_point")
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y + nodeData.height / 2))
								.attr("class", "outPath").attr("self", "showInData").attr("G", gId).attr("arge", o.id)
								.attr("onclick", "_event.addOut_click()");
							var tempcircle = tempg.append("circle").attr("G", gId).attr("r", "5")
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y + nodeData.height / 2))
								.attr("self", "showInData").attr("arge", o.id);
							var temppath = tempg.append("path").attr("self", "showInData")
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y + nodeData.height / 2))
								.attr("d", "M-3,0L3,0M0,3L0,-3").attr("G", gId).attr("arge", o.id);
						} else if(o.state.localeCompare("hide") == 0) {
							var tempg = gmain.append("g").attr("transform", "translate(" + (40 * i) + "," + nodeData.height + ")scale(1,1)")
								.attr("id", o.id + "_point")
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y + nodeData.height / 2))
								.attr("class", "outPath").attr("self", "hideInData").attr("G", gId).attr("arge", o.id)
								.attr("onclick", "_event.addOut_click()");
							var tempcircle = tempg.append("circle").attr("G", gId).attr("r", "5")
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y + nodeData.height / 2))
								.attr("self", "hideInData").attr("arge", o.id);
							var temppath = tempg.append("path").attr("self", "hideInData")
								.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2)).attr("y", (nodeData.y + nodeData.height / 2))
								.attr("d", "M-3,0L3,0").attr("G", gId).attr("arge", o.id);
						}
					}
					return true;
				});
				return false;
			}
			return true;
		});
	},
	_drawDataInPutAndPropertyAndOutPut: function(gId) {
		var nodeData = g.node(gId);
		var hadInput = false;
		var hadOutput = false;
		var g_instance = childsvg.find(gId);
		var aryInput = [];
		var aryOutput = [];

		var property_height = d3.select("#" + gId + "_property_rect").attr("height");
		nodeData.height = _action_node_min_height + parseInt(property_height);
		if(parseInt(property_height) > 0) {
			nodeData.width = _node_max_width;
			nodeData.pro.transform = "translate(" + (_node_max_width - 10) + ",10)scale(1,1)";
		}

		d3.select("#" + gId).selectAll(".inPath")
			.each(function() {
				var tempObj = d3.select(this);
				var tempSelf = tempObj.attr("self");
				var tempId = tempObj.attr("id");
				var tempArge = tempObj.attr("arge");
				var tempG = tempObj.attr("G");
				if(tempSelf.localeCompare("showInData") == 0) {
					aryInput.push({
						id: tempId,
						display: false,
						arge: tempArge,
						G: tempG
					});
				} else if(tempSelf.localeCompare("hideInData") == 0) {
					aryInput.push({
						id: tempId,
						display: true,
						arge: tempArge,
						G: tempG
					});
					hadInput = true;
				}
			});

		d3.select("#" + gId).selectAll(".outPath")
			.each(function() {
				var tempObj = d3.select(this);
				var tempSelf = tempObj.attr("self");
				var tempId = tempObj.attr("id");
				var tempArge = tempObj.attr("arge");
				var tempG = tempObj.attr("G");
				if(tempSelf.localeCompare("showInData") == 0) {
					aryOutput.push({
						id: tempId,
						display: false,
						arge: tempArge,
						G: tempG
					});
				} else if(tempSelf.localeCompare("hideInData") == 0) {
					aryOutput.push({
						id: tempId,
						display: true,
						arge: tempArge,
						G: tempG
					});
					hadOutput = true;
				}
			});

		if(hadInput) {
			nodeData.height = nodeData.height + _node_data_height;
		}

		if(hadOutput) {
			nodeData.height = nodeData.height + _node_data_height;
		}

		if(hadInput || hadInput) {
			nodeData.width = _node_max_width;
			nodeData.pro.transform = "translate(" + (_node_max_width - 10) + ",10)scale(1,1)";
		} else if(parseInt(property_height) == 0) {
			nodeData.width = _action_node_min_width;
			nodeData.pro.transform = "translate(" + (_action_node_min_width - 10) + ",10)scale(1,1)";
		}
		_build._build();

		var g_instance = childsvg.find(gId);
		each(aryInput, function(i, o) {
			var tempData = g_instance.node(this.arge);
			d3.select("#" + this.arge).selectAll("text").remove();
			if(this.display) {
				tempData.width = _node_data_width;
				tempData.height = _node_data_height;
				tempData.rect.width = _node_data_width;
				tempData.rect.height = _node_data_height;
				each(result.nodes, function() {
					if(this.id.localeCompare(gId) == 0) {
						d3.select("#" + o.arge).append("text")
							.attr("G", o.G)
							.attr("class", "nodeChildG_Text").attr("x", 0).attr("y", 8).text("name:");
							
						d3.select("#" + o.arge).append("text")
							.attr("id", o.arge + "_name").attr("G", o.G)
							.attr("class", "nodeChildG_Text").attr("x", 0).attr("y", 18).text(this.inLets[i].name);			
							
						d3.select("#" + o.arge).append("text")
							.attr("G", o.G)
							.attr("class", "nodeChildG_Text").attr("x", 0).attr("y", 29).text("nameData:");	
						var tempTxt = "";
						var tempVal = this.inLets[i].dataName;
						each(dataSetList,function(){
							if(this.k.localeCompare(tempVal)==0){
								tempTxt = this.v.name;
								return false;
							}
							return true;
						});
						d3.select("#" + o.arge).append("text")
							.attr("id", o.arge + "_dataName").attr("G", o.G)
							.attr("class", "nodeChildG_Text").attr("x", 0).attr("y", 39).text(tempTxt);							
						return false;
					}
					return true;
				});
			} else {
				tempData.width = 0;
				tempData.height = 0;
				tempData.rect.width = 0;
				tempData.rect.height = 0;
			}
			return true;
		});

		each(aryOutput, function(i,o) {
			var tempData = g_instance.node(this.arge);
			d3.select("#" + this.arge).selectAll("text").remove();
			if(this.display) {
				tempData.width = _node_data_width;
				tempData.height = _node_data_height;
				tempData.rect.width = _node_data_width;
				tempData.rect.height = _node_data_height;
				
				each(result.nodes, function() {
					if(this.id.localeCompare(gId) == 0) {
						d3.select("#" + o.arge).append("text")
							.attr("G", o.G)
							.attr("class", "nodeChildG_Text").attr("x", 0).attr("y", 8).text("name:");
							
						d3.select("#" + o.arge).append("text")
							.attr("id", o.arge + "_name").attr("G", o.G)
							.attr("class", "nodeChildG_Text").attr("x", 0).attr("y", 18).text(this.outlets[i].name);			
							
						d3.select("#" + o.arge).append("text")
							.attr("G", o.G)
							.attr("class", "nodeChildG_Text").attr("x", 0).attr("y", 29).text("nameData:");	
							
						var tempTxt = "";
						var tempVal = this.outlets[i].dataName;
						each(dataSetList,function(){
							if(this.k.localeCompare(tempVal)==0){
								tempTxt = this.v.name;
								return false;
							}
							return true;
						});
						d3.select("#" + o.arge).append("text")
							.attr("id", o.arge + "_dataName").attr("G", o.G)
							.attr("class", "nodeChildG_Text").attr("x", 0).attr("y", 39).text(tempTxt);							
						return false;
					}
					return true;
				});
			} else {
				tempData.width = 0;
				tempData.height = 0;
				tempData.rect.width = 0;
				tempData.rect.height = 0;
			}
			return true;
		});

		_build._build(gId, g_instance);

		this._drawAboutPosition(gId);
		_draw._drawNodeDataRelation();
	},
	_drawPropertyLeftDiv: function(gId, obj) {
		var nodeData = g.node(gId);
		$("#rightupcssbody").html("");
		d3.select("#divrightup").style({
			"display": "block"
		});
		var notProperty = "@class,id,cmd.class";
		if(obj["cmd.class"]) {
			$.each(obj, function(k, v) {
				var mySelfObj = selfPropertyInfor[gId + "_" + k];
				if(mySelfObj) {
					if(mySelfObj.type.localeCompare("string") == 0) {
						var divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
						drawInputContent(divobj, gId, obj["cmd.class"].toString(), k, v);
					} else if(mySelfObj.type.localeCompare("array") == 0) {
						var divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
						divobj.append("strong").text(k + ":");
						divobj.append("button").text("add +").attr("onclick", "addAndSubInput(1,'" + gId + "','" + k + "')");
						divobj.append("button").text("sub -").attr("onclick", "addAndSubInput(-1,'" + gId + "','" + k + "')");
						if(typeof v == 'string') {
							divobj.append("input").attr("type", "text").attr("placeholder", "...").attr("value", v).on("keyup", function() {
								var ary = [];
								each(this.parentNode.childNodes, function() {
									if(this.tagName.localeCompare("INPUT") == 0) {
										ary.push(this.value);
									}
									return true;
								});
								each(result.nodes, function() {
									if(this.id.localeCompare(gId) == 0) {
										this[k] = ary;
										_draw._drawPropertyLeftDiv(gId, this);
										//_draw._drawProperty(gId, this);
										return false;
									}
									return true;
								});
							});
						} else {
							each(v, function() {
								divobj.append("input").attr("type", "text").attr("placeholder", "...").attr("value", this).on("keyup", function() {
									var ary = [];
									each(this.parentNode.childNodes, function() {
										if(this.tagName.localeCompare("INPUT") == 0) {
											ary.push(this.value);
										}
										return true;
									});
									each(result.nodes, function() {
										if(this.id.localeCompare(gId) == 0) {
											this[k] = ary;
											//_draw._drawProperty(gId, this);
											_draw._drawPropertyLeftDiv(gId, this);
											return false;
										}
										return true;
									});
								});
								return true;
							});
						}
					}
				} else {
					if(typeof v == 'string') {
						if(notProperty.indexOf(k) == -1) {
							if(k.localeCompare("name") == 0) {
								$("#rightupcssheaderstring").html(v);
							}
							var divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
							drawInputContent(divobj, gId, obj["cmd.class"].toString(), k, v);
						}
					} else if(typeof v == 'object') {
						if(k.localeCompare("inLets") != 0 && k.localeCompare("outlets") != 0) {
							var divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
							drawInputContent(divobj, gId, obj["cmd.class"].toString(), k, v);
						}
					}
				}
			});
		} else if(obj["name"].toString().localeCompare("start") == 0) {
			var divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
			divobj.append("strong").text("name:");
			divobj.append("input").attr("type", "text").attr("value", "start").attr("placeholder", "start...").attr("onkeyup", "changeProperty('" + gId + "','name')");

			divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");

			divobj.append("strong").text("duration:");
			divobj.append("input").attr("type", "number")
				.attr("value", "500")
				.attr("placeholder", "you need input integer...")
				.attr("onchange", "changeProperty('" + gId + "','duration')");
		}else if(obj["name"].toString().localeCompare("end")==0){
			var divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
			divobj.append("strong").text("name:");
			divobj.append("input").attr("type", "text").attr("value", "start").attr("placeholder", "start...").attr("onkeyup", "changeProperty('" + gId + "','name')");			
		}

		//单个属性
		var divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
		divobj.append("input").attr("type", "text").attr("placeholder", "format  k:v  ...").style({
			background: 'silver'
		}).on("dblclick", function() {
			var tempValue = this.value;
			if(tempValue) {
				if(tempValue.indexOf(":") > -1) {
					tempValue = tempValue.split(":");
					each(result.nodes, function(i, o) {
						if(this.id.localeCompare(gId) == 0) {
							this[tempValue[0].trim()] = tempValue[1].trim();
							selfPropertyInfor[gId + "_" + tempValue[0].trim()] = {
								type: 'string'
							};
							_draw._drawPropertyLeftDiv(gId, obj);
							return false;
						}
						return true;
					});
				}
			}
			this.value = "";
		});

		divobj = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
		divobj.append("input").attr("id", "self_propertyName").attr("type", "text").attr("placeholder", "k...").style({
			background: 'silver',
			width: "35%"
		})
		divobj.append("button").text("add +").attr("onclick", "addAndSubInput(1,'" + gId + "')");
		divobj.append("button").text("sub -").attr("onclick", "addAndSubInput(-1,'" + gId + "')");
		divobj.append("button").text("save").attr("onclick", "saveSelfArrayProperty('" + gId + "')");

		this._drawProperty(gId, obj);
	},
	_drawProperty: function(gId, obj) {
		//画 property 哈哈哈哈
		var _index = 0;
		var notProperty = "@class,id,cmd.class";
		$.each(obj, function(k, v) {
			if((k.localeCompare("inLets") != 0) && (k.localeCompare("outlets") != 0)) {
				if(notProperty.indexOf(k) == -1) {
					if(v.length > 0) {
						_index++;
					}
				}
			}
		});
		var g_child_Instance = childsvg.find(gId);
		if(g_child_Instance == null) {
			g_child_Instance = childsvg.initNewInstance(gId);
		}

		var propertyData = g_child_Instance.node(gId + "_property");
		propertyData.width = _node_max_property_width;
		propertyData.height = (_index * 20);
		propertyData.rect.width = _node_max_property_width;
		propertyData.rect.height = (_index * 20);

		d3.select("#" + gId + "_property").selectAll("text").remove();
		var property_no_txt = "@class,id,cmd.class";
		var txt_index = 0;
		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				$.each(this, function(k, v) {
					var temp_k = k.toString().replaceAll(".", "");
					if(k.localeCompare("inLets")!=0&&k.localeCompare("outlets")!=0){
						if(v.length > 0) {
							if(property_no_txt.indexOf(k) == -1) {
								txt_index++;
								var finaleTxt = k + ":" + v;
								var tempMaxLength = 35;
								if(finaleTxt.length > 35) {
									finaleTxt = finaleTxt.substring(0, 30) + "...";
								}
								d3.select("#" + gId + "_property")
									.append("text").attr("id", gId + "_property_" + temp_k)
									.attr("x", 0).attr("y", (txt_index * 20) - 5).text(finaleTxt);
							}
						}						
					}
				});
				return false;
			}
			return true;
		});

		_build._build(gId, g_child_Instance);
		//this._drawAboutPosition(gId);
		_draw._drawDataInPutAndPropertyAndOutPut(gId);
	},
	_drawClearProperty: function(gId) {
		$("#rightupcssbody").html("");
		d3.select("#divrightup").style({
			"display": "none"
		});
		var tempheight = d3.select("#" + gId + "_property_rect").attr("height");
		var hadInData = false;
		var hadOutData = false;
		d3.select("#" + gId).selectAll(".inPath")
			.each(function() {
				var temp = d3.select(this).attr("self");
				if(temp.localeCompare("hideInData") == 0) {
					hadInData = true;
				}
			});
		d3.select("#" + gId).selectAll(".outPath")
			.each(function() {
				var temp = d3.select(this).attr("self");
				if(temp.localeCompare("hideInData") == 0) {
					hadOutData = true;
				}
			});
		d3.select("#" + gId + "_property").selectAll("text").remove();
		var nodeData = g.node(gId);
		nodeData.pro.self = "ShowProperty";
		nodeData.pro.circle.self = "ShowProperty";
		nodeData.pro.path.self = "ShowProperty";
		nodeData.pro.path.d = "M-3,0L3,0M0,-3L0,3";
		if((!hadInData) && (!hadOutData)) {
			if(nodeData.state.localeCompare("start") == 0) {
				//开始
				nodeData.width = _start_node_min_width;
				nodeData.pro.transform = "translate(" + (_start_node_min_width - 10) + ",10)scale(1,1)";
			} else if(nodeData.state.localeCompare("end") == 0) {
				//结束
				nodeData.width = _end_node_min_width;
				nodeData.pro.transform = "translate(" + (_end_node_min_width - 10) + ",10)scale(1,1)";
			} else {
				nodeData.width = _action_node_min_width;
				nodeData.pro.transform = "translate(" + (_action_node_min_width - 10) + ",10)scale(1,1)";
			}
			nodeData.height = _action_node_min_height;

			_build._build();

			var g_instance = childsvg.find(gId);
			var pro_data = g_instance.node(gId + "_property");
			pro_data.width = 0;
			pro_data.height = 0;
			pro_data.rect.width = 0;
			pro_data.rect.height = 0;
			_build._build(gId, g_instance);
		} else {
			var g_instance = childsvg.find(gId);
			var pro_data = g_instance.node(gId + "_property");
			nodeData.height = nodeData.height - pro_data.height;
			_build._build();

			pro_data.width = 0;
			pro_data.height = 0;
			pro_data.rect.width = 0;
			pro_data.rect.height = 0;
			_build._build(gId, g_instance);
		}

		this._drawAboutPosition(gId);
	},
	_drawAboutPosition: function(gId) {
		setTimeout(function() {
			var nodeData = g.node(gId);
			//定位下方的按钮
			each(result.nodes, function() {
				if(this.id.localeCompare(gId) == 0) {
					each(this.outlets, function(i, o) {
						d3.select("#" + o.id + "_point").attr("transform", "translate(" + (i * 40) + "," + nodeData.height + ")scale(1,1)");
						return true;
					});
					return false;
				}
				return true;
			});
		}, 750);
	},
	_drawNodeDataRelation: function() {
		d3.select("#lineContainer").selectAll(".edgeData").remove();
		each(result.links, function() {
			var fromPoint = [];
			var toPoint = [];
			var fromtxt, totxt;
			fromtxt = this.fromNodeName;
			totxt = this.toNodeName;
			//----------------------------------开始的出发画线--------------------------------
			each(result.nodes, function() {
				if(this.id.localeCompare(fromtxt) == 0) {
					each(this.outlets, function() {
						if(this.show) {
							if(this.dataName.length > 0) {
								var tempX = d3.select("#" + this.id + "_point").attr("x");
								var tempY = d3.select("#" + this.id + "_point").attr("y");
								fromPoint.push({
									x: tempX,
									y: tempY,
									dataName: this.dataName
								});
							}
						}
						return true;
					});
					return false;
				}
				return true;
			});
			//-----------------------------------------结束----------------------------------------------------

			//----------------------------------结束的画线--------------------------------
			if(fromPoint.length > 0) {
				each(result.nodes, function() {
					if(this.id.localeCompare(totxt) == 0) {
						each(this.inLets, function() {
							if(this.show) {
								if(this.dataName.length > 0) {
									var tempX = d3.select("#" + this.id + "_point").attr("x");
									var tempY = d3.select("#" + this.id + "_point").attr("y");
									toPoint.push({
										x: tempX,
										y: tempY,
										dataName: this.dataName
									})
								}
							}
							return true;
						});
						return false;
					}
					return true;
				});
			}
			//-----------------------------------------结束----------------------------------------------------

			//确定点的位置
			if(fromPoint.length > 0 && toPoint.length > 0) {
				each(fromPoint, function() {
					var from_x = this.x;
					var from_y = this.y;
					var from_DataName = this.dataName;
					each(toPoint, function() {
						if(this.dataName.localeCompare(from_DataName) == 0) {
							var points = [];
							points.push({
								x: parseInt(from_x),
								y: parseInt(from_y)
							});

							points.push({
								x: (parseInt(from_x) + parseInt(this.x)) / 2,
								y: (parseInt(from_y) + parseInt(this.y)) / 2,
							});

							points.push({
								x: parseInt(this.x),
								y: parseInt(this.y)
							});
							console.log(points);
							d3.select("#lineContainer").append("path")
								.attr("class", "edgeData").attr("d", interpolate(points));
						}
						return true;
					});
					return true;
				});
			}
			return true;
		});
	}
}

/**
 * 
 * @param {Object} divObj
 * @param {Object} gId
 * @param {Object} actionName
 * @param {Object} propertyName
 * @param {Object} propertyValue
 */
var drawInputContent = function(divObj, gId, actionName, propertyName, propertyValue) {
	var notProperty = "@class,id,cmd.class";
	each(propertyInfor, function() {
		if(notProperty.indexOf(propertyName) == -1) {
			if(this["cmd.class"]) {
				if(this["cmd.class"].default) {
					if(this["cmd.class"].default.toString().indexOf(actionName) > -1) {
						if(this[propertyName]) {
							console.log(this);
							var obj = this[propertyName];
							if(obj["enum"]) {
								divObj.append("strong").text(propertyName + ":");
								var selectedObj = divObj.append("select").attr("onchange", "changeProperty('" + gId + "','" + propertyName + "')");
								each(obj["enum"], function(i, o) {
									if(this.toString().localeCompare(obj["default"].toString()) == 0) {
										selectedObj.append("option").attr("selected", "selected")
											.attr("value", this).text(this);
									} else {
										selectedObj.append("option").attr("value", this).text(this);
									}
									return true;
								});
							} else if(obj["type"].toString().localeCompare("string") == 0) {
								if(propertyValue.length == 0) {
									if(obj.default) {
										propertyValue = obj.default;
									}
								}
								divObj.append("strong").text(propertyName + ":");
								divObj.append("input").attr("type", "text").attr("value", propertyValue).attr("placeholder", "...")
									.attr("onkeyup", "changeProperty('" + gId + "','" + propertyName + "')");
							} else if(obj["type"].toString().localeCompare("array") == 0) {
								divObj.append("strong").text(propertyName + ":");
								divObj.append("button").text("add +").attr("onclick", "addAndSubInput(1,'" + gId + "','" + propertyName + "')");
								divObj.append("button").text("sub -").attr("onclick", "addAndSubInput(-1,'" + gId + "','" + propertyName + "')");
								if(typeof propertyValue == 'string') {
										divObj.append("input").attr("type", "text").attr("placeholder", "...").attr("value", propertyValue).on("keyup", function() {
											var ary = [];
											each(this.parentNode.childNodes, function() {
												if(this.tagName.localeCompare("INPUT") == 0) {
													ary.push(this.value);
												}
												return true;
											});
											each(result.nodes, function() {
												if(this.id.localeCompare(gId) == 0) {
													this[propertyName] = ary;
													_draw._drawProperty(gId, this);
													return false;
												}
												return true;
											});
										});
								} else {
									each(propertyValue, function() {
										divObj.append("input").attr("type", "text").attr("placeholder", "...").attr("value", this).on("keyup", function() {
											var ary = [];
											each(this.parentNode.childNodes, function() {
												if(this.tagName.localeCompare("INPUT") == 0) {
													ary.push(this.value);
												}
												return true;
											});
											each(result.nodes, function() {
												if(this.id.localeCompare(gId) == 0) {
													this[propertyName] = ary;
													_draw._drawProperty(gId, this);
													return false;
												}
												return true;
											});
										});
										return true;
									});
								}

							} else if(obj["type"].toString().localeCompare("boolean") == 0) {
								if(propertyValue.length > 0) {
									if(propertyValue.localeCompare("true") == 0) {
										divObj.append("input").attr("type", "checkbox").attr("checked", propertyValue).attr("placeholder", "you need input array...")
											.attr("onchange", "changeProperty('" + gId + "','" + propertyName + "')");
									} else {
										divObj.append("input").attr("type", "checkbox").attr("placeholder", "you need input array...")
											.attr("onchange", "changeProperty('" + gId + "','" + propertyName + "')");
									}
								} else {
									divObj.append("input").attr("type", "checkbox").attr("placeholder", "you need input array...")
										.attr("onchange", "changeProperty('" + gId + "','" + propertyName + "')");
								}

								divObj.append("strong").text(propertyName + ":");
							} else if(obj["type"].toString().localeCompare("integer") == 0) {
								if(propertyValue.length == 0) {
									if(obj["default"]) {
										propertyValue = obj["default"];
									} else {
										propertyValue = "0";
									}
								}
								divObj.append("strong").text(propertyName + ":");
								divObj.append("input").attr("type", "number")
									.attr("value", propertyValue)
									.attr("placeholder", "you need input integer...")
									.attr("onchange", "changeProperty('" + gId + "','" + propertyName + "')");
							}
						} else {
							divObj.append("strong").text(propertyName + ":");
							divObj.append("input").attr("type", "text").attr("value", propertyValue).attr("placeholder", "...")
								.attr("onkeyup", "changeProperty('" + gId + "','" + propertyName + "')");
						}
						return false;
					}
				}
			}
		}
		return true;
	});
}

var dataSetList = [];

var addLeftDiv = function(keys, obj) {
	if(obj) {
		dataSetList.unshift({
			k: keys,
			show: false,
			v: obj
		});
		result.data.push(obj);
	} else {
		dataSetList.unshift({
			k: keys,
			show: false,
			v: {
				name: 'data',
				location: '',
				dataFormat: 'Line',
				recordType: 'Path',
				instance: 'false'
			}
		});
		
		result.data.push({
			id: keys,
			name: 'data',
			location: '',
			dataFormat: 'Line',
			recordType: 'Path',
			instance: 'false'
		});
	}

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
		
		each(selfDataInfor["dataFormat"].v , function(){
			txt += "<option value='"+this+"'>"+this+"</option>"; 
			return true;
		});
		txt+="</select>";
			
//		txt += "<option value='Line'>Line</option><option value='XML'>XML</option><option value='Text'>Text</option><option value='Binary'>Binary</option>";
//		txt += "<option value='Section'>Section</option><option value='Mixed'>Mixed</option><option value='FileName'>FileName</option></select>";
//		
		//txt += "<strong>recordType:</strong><select args='recordType' style='width: 100%;' onchange='changeDataSetProeroty(\"" + o.k + "\")'>";
		
		txt += "<strong>recordType:</strong><select args='recordType' style='width: 100%;' onchange='changeDataSetProeroty(\"" + o.k + "\")'>";
		each(selfDataInfor["recordType"].v , function(){
			txt += "<option value='"+this+"'>"+this+"</option>"; 
			return true;
		});		
		txt+="</select>";			
		
		
//		txt += "<option value='Path'>Path</option><option value='KeyPath'>KeyPath</option><option value='Value'>Value</option><option value='KeyValue'>KeyValue</option></select>";
//		
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
}

var saveSelfArrayProperty = function(gId) {
	var propertyName = document.getElementById("self_propertyName").value;
	if(propertyName) {
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		var ary = [];
		each(o.parentNode.childNodes, function() {
			if(this.tagName.localeCompare("INPUT") == 0) {
				if(this.id) {

				} else {
					ary.push(this.value);
				}
			}
			return true;
		});
		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				this[propertyName] = ary;
				selfPropertyInfor[gId + "_" + propertyName] = {
					type: 'array'
				};
				//_draw._drawProperty(gId, this);
				_draw._drawPropertyLeftDiv(gId, this);
				return false;
			}
			return true;
		});
		while(true) {
			var objInput = o.parentNode.childNodes[o.parentNode.childNodes.length - 1];
			if(objInput.tagName.localeCompare("INPUT") == 0) {
				o.parentNode.removeChild(objInput);
			} else {
				break;
			}
		}
		console.log(result);
	}
}

var addAndSubInput = function(direction, gId, propertyName) {
	var e = window.event || arguments.callee.caller.arguments[0];
	var o = getEventSources(e);
	e.stopPropagation();
	if(direction > 0) {
		console.log(o.parentNode);
		var objInput = document.createElement("input");
		objInput.setAttribute("type", "text");
		objInput.setAttribute("placeholder", "...");
		if(propertyName) {
			objInput.addEventListener("keyup", function() {
				var ary = [];
				each(o.parentNode.childNodes, function() {
					if(this.tagName.localeCompare("INPUT") == 0) {
						ary.push(this.value);
					}
					return true;
				});
				each(result.nodes, function() {
					if(this.id.localeCompare(gId) == 0) {
						this[propertyName] = ary;
						_draw._drawProperty(gId, this);
						return false;
					}
					return true;
				});
				console.log(result);
			});
			o.parentNode.appendChild(objInput);
		}else {
			o.parentNode.appendChild(objInput);
			o.parentNode.parentNode.scrollTop = o.parentNode.parentNode.scrollHeight;
		}
	} else if(direction < 0) {
		console.log(o.parentNode);
		var objInput = o.parentNode.childNodes[o.parentNode.childNodes.length - 1];
		if(objInput.tagName.localeCompare("INPUT") == 0) {
			o.parentNode.removeChild(objInput);
			each(result.nodes, function() {
				if(this.id.localeCompare(gId) == 0) {
					if(propertyName) {
						if(typeof this[propertyName] == 'string'){
							console.log("this[propertyName]", this[propertyName]);
							this[propertyName] = '';
						}else {
							console.log("this[propertyName]", this[propertyName]);
							this[propertyName].splice(this[propertyName].length - 1, 1);
						}
					}
					_draw._drawProperty(gId, this);
					return false;
				}
				return true;
			});
			
		}
	}
}

var changeProperty = function(gId, propertyName) {
	var e = window.event || arguments.callee.caller.arguments[0];
	var o = getEventSources(e);
	if(propertyName.localeCompare("name") == 0) {
		$("#rightupcssheaderstring").html(o.value);
		d3.select("#" + gId).select("text").text(o.value);
	}
	if(o.tagName.localeCompare("INPUT") == 0 && o.type.localeCompare("checkbox") == 0) {
		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				this[propertyName] = o.checked.toString();
				var tempPropertyName = propertyName.replaceAll(".", "");
				d3.select("#" + gId + "_property").select("#" + gId + "_property_" + tempPropertyName).text(propertyName + ":" + o.checked);

				_draw._drawProperty(gId, this);
				return false;
			}
			return true;
		});
	} else if(o.tagName.localeCompare("INPUT") == 0) {
		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				this[propertyName] = o.value;
				var tempPropertyName = propertyName.replaceAll(".", "");
				d3.select("#" + gId + "_property").select("#" + gId + "_property_" + tempPropertyName).text(propertyName + ":" + o.value);

				_draw._drawProperty(gId, this);
				return false;
			}
			return true;
		});
	} else if(o.tagName.localeCompare("SELECT") == 0) {
		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				this[propertyName] = o.value;
				var tempPropertyName = propertyName.replaceAll(".", "");
				d3.select("#" + gId + "_property").select("#" + gId + "_property_" + tempPropertyName).text(propertyName + ":" + o.value);

				_draw._drawProperty(gId, this);
				return false;
			}
			return true;
		});
	}
}

/**
 * 
 * @param {Object} txtId
 */
var changeData = function(txtId, propertyName, gId) {
	var e = window.event || arguments.callee.caller.arguments[0];
	var o = getEventSources(e);

	if(o.tagName.localeCompare("INPUT") == 0) {
		d3.select("#rightupcssheaderstring").text(o.value);
		d3.select("#" + txtId + "_" + propertyName).text(o.value);
		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				each(this.inLets, function() {
					if(this.id.localeCompare(txtId) == 0) {
						this.name = o.value;
						return false;
					}
					return true;
				});
				each(this.outlets, function() {
					if(this.id.localeCompare(txtId) == 0) {
						this.name = o.value;
						return false;
					}
					return true;
				});
				return false;
			}
			return true;
		});
	} else if(o.tagName.localeCompare("SELECT") == 0) {
		d3.select("#" + txtId + "_" + propertyName).text(o.selectedOptions[0].text);
		each(result.nodes, function() {
			if(this.id.localeCompare(gId) == 0) {
				each(this.inLets, function() {
					if(this.id.localeCompare(txtId) == 0) {
						this.dataName = o.value;
						return false;
					}
					return true;
				});
				each(this.outlets, function() {
					if(this.id.localeCompare(txtId) == 0) {
						this.dataName = o.value;
						return false;
					}
					return true;
				});
				return false;
			}
			return true;
		});

		_draw._drawNodeDataRelation();
	}
}