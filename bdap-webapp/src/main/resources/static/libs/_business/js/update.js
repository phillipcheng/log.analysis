var update = function(theSelf, d, nodeData) {
	if(nodeData.state.localeCompare("start") == 0 || nodeData.state.localeCompare("end") == 0 || nodeData.state.localeCompare("action") == 0) {
		var theSelfObj = d3.select(theSelf);
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");

		var rectData = nodeData.rect;
		theSelfObj.select("#" + rectData.id) //rect
			.transition().duration(_TRANSITION_DURATION)
			.attr("width", nodeData.width)
			.attr("height", nodeData.height);

		var txtData = nodeData.txt;
		var txtObj = theSelfObj.select("#" + txtData.id);
		txtObj.transition().duration(_TRANSITION_DURATION).attr("x", txtData.x).attr("y", txtData.y).text(txtData.txt);

		if(nodeData.run) {
			var runData = nodeData.run;
			var temp = nodeData.width - 15;
			temp += ",";
			temp += nodeData.height - 15;
			var runObj = theSelfObj.select("#" + d + "_g_run");
			runObj.transition().duration(_TRANSITION_DURATION)
				.attr("transform", "translate(" + temp + ")scale(1,1)");
		}

		var txtData = nodeData.txt;
		var txtObj = theSelfObj.select("#" + txtData.id)
		txtObj.transition().duration(_TRANSITION_DURATION).attr("x", txtData.x).attr("y", txtData.y)
			.text(txtData.txt);

		var proData = nodeData.pro;
		var proObj = theSelfObj.select("#" + proData.id);
		proObj.transition().duration(_TRANSITION_DURATION).attr("self", proData.self).attr("transform", proData.transform);

		var proObjCircle = theSelfObj.select("#" + proData.circle.id);
		proObjCircle.transition().duration(_TRANSITION_DURATION).attr("self", proData.circle.self);

		var proObjPath = theSelfObj.select("#" + proData.path.id);
		proObjPath.transition().duration(_TRANSITION_DURATION).attr("self", proData.path.self).attr("d", proData.path.d);

		if(nodeData.addIn) {
			var addInData = nodeData.addIn;
			var addInObj = theSelfObj.select("#" + addInData.id);
			addInObj.transition().duration(_TRANSITION_DURATION).attr("self", addInData.self).attr("transform", addInData.transform);

			var addInObjCircle = theSelfObj.select("#" + addInData.circle.id);
			addInObjCircle.transition().duration(_TRANSITION_DURATION).attr("self", addInData.circle.self);

			var addInObjPath = theSelfObj.select("#" + addInData.path.id);
			addInObjPath.transition().duration(_TRANSITION_DURATION).attr("self", addInData.path.self);
		}

		if(nodeData.addOut) {
			var addOutData = nodeData.addOut;
			var addOutObj = theSelfObj.select("#" + addOutData.id);
			addOutObj.transition().duration(_TRANSITION_DURATION).attr("self", addOutData.self).attr("transform", addOutData.transform);

			var addOutObjCircle = theSelfObj.select("#" + addInData.circle.id);
			addOutObjCircle.transition().duration(_TRANSITION_DURATION).attr("self", addOutData.circle.self);

			var addOutObjPath = theSelfObj.select("#" + addOutData.path.id);
			addOutObjPath.transition().duration(_TRANSITION_DURATION).attr("self", addOutData.path.self);
		}

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
		var theSelfObj = d3.select(theSelf);
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");
		if(nodeData.display) {
			if(theSelfObj.select("rect")[0][0]) {
				theSelfObj.select("rect").attr("width", nodeData.width).attr("height", nodeData.height);
			} else {
				theSelfObj.append("rect").attr("width", nodeData.width).attr("height", nodeData.height);
			}
		}
	} else if(nodeData.state.localeCompare("inData") == 0) {
		var theSelfObj = d3.select(theSelf);
		theSelfObj.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");
		theSelfObj.select("rect").attr("width", nodeData.width).attr("height", nodeData.height);
	} else if(nodeData.state.localeCompare("property") == 0) {
		if(nodeData.display) {

		} else {
			d3.select("#property_container_" + d).selectAll("g")
			each(function() {
				d3.select(this).remove();
			});
		}
	} else if(nodeData.state.localeCompare("outData") == 0) {
		var theSelfObj = d3.select(theSelf);
		theSelfObj.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");
		theSelfObj.select("rect").attr("width", nodeData.width).attr("height", nodeData.height);
	}
}

var smallNodeUpdate = function(theSelf, d, nodeData) {
	if(nodeData.state.localeCompare("start") == 0 ||
		nodeData.state.localeCompare("end") == 0 ||
		nodeData.state.localeCompare("action") == 0) {
		var theSelfObj = d3.select(theSelf);
		theSelfObj.attr("cx", ((nodeData.x / 20) - 1)).attr("cy", ((nodeData.y / 20) - 1));
	}
}

var childNodeUpdate = function(theSelf, d, nodeData) {
	var theSelfObj = d3.select(theSelf);
	var tempId = nodeData.id;
	var nodeParentData = g.node(nodeData.G);
	if(tempId.indexOf("_InData_0") > -1) {
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(0," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");
	} else if(tempId.indexOf("_InData_1") > -1) {
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(65," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");

	} else if(tempId.indexOf("_InData_2") > -1) {
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(130," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");

	} else if(tempId.indexOf("_InData_3") > -1) {
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(195," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");

	} else if(tempId.indexOf("_OutData_0") > -1) {
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(0," + (nodeParentData.height - 80) + ")scale(1,1)");
	} else if(tempId.indexOf("_OutData_1") > -1) {
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(65," + (nodeParentData.height - 80) + ")scale(1,1)");
	} else if(tempId.indexOf("_OutData_2") > -1) {
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(130," + (nodeParentData.height - 80) + ")scale(1,1)");
	} else if(tempId.indexOf("_OutData_3") > -1) {
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(195," + (nodeParentData.height - 80) + ")scale(1,1)");
	} else if(tempId.indexOf("_property") > -1) {
		theSelfObj.transition().duration(_TRANSITION_DURATION)
			.attr("transform", "translate(0," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");
	}

	if(nodeData.rect) {
		var theSelfObjRect = theSelfObj.select("#" + nodeData.rect.id);
		$.each(nodeData.rect, function(k, v) {
			theSelfObjRect.attr(k, v);
		});
	}

}