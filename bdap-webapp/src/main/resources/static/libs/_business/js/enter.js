var enter = function(theSelf, d, nodeData) {
	if(nodeData.state.localeCompare("start") == 0 || nodeData.state.localeCompare("end") == 0 || nodeData.state.localeCompare("action") == 0) {
		var theSelfObj = d3.select(theSelf);
		theSelfObj.attr("id", nodeData.id).attr("class", nodeData.class).attr("transform", nodeData.transform);

		var rectData = nodeData.rect;
		var rectObj = theSelfObj.append("rect");
		rectObj.attr("G", d);
		$.each(rectData, function(k, v) {
			rectObj.attr(k, v);
		});

		if(nodeData.run) {
			var runData = nodeData.run;
			var temp = nodeData.width - 15;
			temp += ",";
			temp += nodeData.height - 15;
			var runObj = theSelfObj.append("g").attr("id", d + "_g_run").attr("transform", "translate(" + temp + ")scale(1,1)").append("path");
			runObj.attr("G", d);
			$.each(runData, function(k, v) {
				runObj.attr(k, v);
			});
		}

		if(nodeData.txt) {
			var txtData = nodeData.txt;
			var txtObj = theSelfObj.append("text");
			txtObj.attr("G", d);
			$.each(txtData, function(k, v) {
				if(k.toString().localeCompare("txt") == 0) {
					txtObj.text(v);
				} else {
					txtObj.attr(k, v);
				}
			});
		}

		var proData = nodeData.pro;
		var proObj = theSelfObj.append("g");
		var proObjCircle = proObj.append("circle");
		var proObjPath = proObj.append("path");

		proObj.attr("G", d);
		proObjCircle.attr("G", d);
		proObjPath.attr("G", d);

		$.each(proData, function(k, v) {
			if(typeof v != 'object') {
				proObj.attr(k, v);
			} else if(k.localeCompare("circle") == 0) {
				$.each(proData.circle, function(ck, cv) {
					proObjCircle.attr(ck, cv);
				});
			} else if(k.localeCompare("path") == 0) {
				$.each(proData.path, function(pk, pv) {
					proObjPath.attr(pk, pv);
				});
			}
		});

		if(nodeData.addIn) {
			var addInObj = theSelfObj.append("g");
			var addInObjCircle = addInObj.append("circle");
			var addInObjPath = addInObj.append("path");

			addInObj.attr("G", d);
			addInObjCircle.attr("G", d);
			addInObjPath.attr("G", d);

			$.each(nodeData.addIn, function(k, v) {
				if(typeof v != 'object') {
					addInObj.attr(k, v);
				} else if(k.localeCompare("circle") == 0) {
					$.each(nodeData.addIn.circle, function(ck, cv) {
						addInObjCircle.attr(ck, cv);
					});
				} else if(k.localeCompare("path") == 0) {
					$.each(nodeData.addIn.path, function(pk, pv) {
						addInObjPath.attr(pk, pv);
					});
				}
			});
		}

		if(nodeData.addOut) {

			var addOutObj = theSelfObj.append("g");
			var addOutObjCircle = addOutObj.append("circle");
			var addOutObjPath = addOutObj.append("path");

			addOutObj.attr("G", d);
			addOutObjCircle.attr("G", d);
			addOutObjPath.attr("G", d);

			$.each(nodeData.addOut, function(k, v) {
				if(typeof v != 'object') {
					addOutObj.attr(k, v);
				} else if(k.localeCompare("circle") == 0) {
					$.each(nodeData.addOut.circle, function(ck, cv) {
						addOutObjCircle.attr(ck, cv);
					});
				} else if(k.localeCompare("path") == 0) {
					$.each(nodeData.addOut.path, function(pk, pv) {
						addOutObjPath.attr(pk, pv);
					});
				}
			});
		}

		//add child svg
		theSelfObj.append("g").attr("transform", "translate(15,30)scale(1,1)")
			.attr("id", d + "_svg").append("svg").attr("id", d + "_g_svg")
			.append("g").attr("id", "rectChildContainer").attr("transform", "translate(1,1)scale(1,1)");

		//添加那8个数据点的x,y
		if(nodeData.state.localeCompare("start") == 0) {
			each(result.nodes, function() {
				if(this.id.localeCompare(d) == 0) {
					each(this.outlets, function(i, o) {
						theSelfObj.select("#" + o.id + "_point")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y + nodeData.height / 2));
						theSelfObj.select("#" + o.id + "_point").select("circle")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y + nodeData.height / 2));
						theSelfObj.select("#" + o.id + "_point").select("path")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y + nodeData.height / 2));							
						return true;
					});
				}
				return true;
			});
		} else if(nodeData.state.localeCompare("end") == 0) {
			each(result.nodes, function() {
				if(this.id.localeCompare(d) == 0) {
					each(this.inLets, function(i, o) {
						theSelfObj.select("#" + o.id + "_point")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y - nodeData.height / 2));
						theSelfObj.select("#" + o.id + "_point").select("circle")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y - nodeData.height / 2));
						theSelfObj.select("#" + o.id + "_point").select("path")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y - nodeData.height / 2));							
						return true;
					});
				}
				return true;
			});
		} else {
			each(result.nodes, function() {
				if(this.id.localeCompare(d) == 0) {
					each(this.inLets, function(i, o) {					
						theSelfObj.select("#" + o.id + "_point")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y - nodeData.height / 2));
						theSelfObj.select("#" + o.id + "_point").select("circle")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y - nodeData.height / 2));
						theSelfObj.select("#" + o.id + "_point").select("path")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y - nodeData.height / 2));							
						return true;
					});
				}
				return true;
			});
			each(result.nodes, function() {
				if(this.id.localeCompare(d) == 0) {
					each(this.outlets, function(i, o) {
						theSelfObj.select("#" + o.id + "_point")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y + nodeData.height / 2));
						theSelfObj.select("#" + o.id + "_point").select("circle")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y + nodeData.height / 2));
						theSelfObj.select("#" + o.id + "_point").select("path")
							.attr("x", (nodeData.x + (40 * i) - nodeData.width / 2))
							.attr("y", (nodeData.y + nodeData.height / 2));							
						return true;
					});
				}
				return true;
			});
		}

	} else if(nodeData.state.localeCompare("group") == 0) {

	} else if(nodeData.state.localeCompare("inData") == 0) {

	} else if(nodeData.state.localeCompare("property") == 0) {

	} else if(nodeData.state.localeCompare("outData") == 0) {

	}
}

var smallNodeEnter = function(theSelf, d, nodeData) {
	var theSelfObj = d3.select(theSelf);
	theSelfObj.attr("class", "nodeSmallG")
		.attr("id", "small" + d)
		.attr("r", 2)
		.attr("cx", ((nodeData.x / 20) - 1))
		.attr("cy", ((nodeData.y / 20) - 1));
}

var childNodeEnter = function(theSelf, d, nodeData) {
	var theSelfObj = d3.select(theSelf);

	$.each(nodeData, function(k, v) {
		if(typeof v == 'string') {
			theSelfObj.attr(k, v);
		}
	});
	//	theSelfObj.attr("id", nodeData.id).attr("G", nodeData.G)
	//		.attr("class", nodeData.class).attr("transform", "translate(0,0)scale(1,1)");

	if(nodeData.rect) {
		var theSelfObjRect = theSelfObj.append("rect");
		$.each(nodeData.rect, function(k, v) {
			theSelfObjRect.attr(k, v);
		});
	}
}