/**
 *  全局的键盘 按下事件
 * @param {Object} event
 */
document.onkeydown = function(event) {
	var e = event || window.event || arguments.callee.caller.arguments[0];
	console.log(e);
	if(e && e.keyCode == 27) { // 按 Esc 取消操作
		clearTempLine();
		document.getElementById("svg").style.cursor = "default";
	}

	if(e && e.keyCode == 46) { // 按 delete 取消操作
		var json = dagre.graphlib.json.write(g);
		console.info(json);
		d3.select("#rectContainer").selectAll(".nodeRectSelected")
			.each(function() {
				var gId = d3.select(this).attr("G").toString();
				console.log("gId", gId);
				g.removeNode(gId);
				each(pathLists, function(i, o) {

					if(o.from.toString().localeCompare(gId) == 0) {
						g.removeEdge(gId, o.to.toString());
						d3.select("#linegroup" + gId + "A" + o.to.toString()).remove();
					} else if(o.to.toString().localeCompare(gId) == 0) {
						g.removeEdge(o.from.toString(), gId);
						d3.select("#linegroup" + o.from.toString() + "A" + gId).remove();
					}

					if(o.from.toString().localeCompare(gId) == 0 ||
						o.to.toString().localeCompare(gId) == 0) {
						pathLists.splice(i, 1);
						return --i;
					} else {
						return true;
					}

				});
				d3.select("#" + gId).remove();
				console.log(pathLists);
			});

		d3.select("#lineContainer").selectAll(".edgeSelected")
			.each(function() {
				var gId = d3.select(this).attr("id").toString();
				var temp = gId.replace("linegroup", "");
				temp = temp.split("A");
				each(pathLists, function(i, o) {
					if(o.from.toString(o.from.toString()).localeCompare() == 0 && o.to.toString().localeCompare(o.to.toString()) == 0) {
						g.removeEdge(gId, o.to.toString());
						pathLists.splice(i, 1);
						return --i;
					} else {
						return true;
					}
				});
			}).remove();

		console.info(dagre.graphlib.json.write(g));
	}

	if(e && e.keyCode == 82) { // 按 R 取消操作
		_base._build();
	}

}

/**
 * 
 * @param {Object} event
 */
//document.ondblclick = function(event) {
//	console.log("--------------ondblclick----------------");
//	event.stopPropagation();
//	clearTimeout(clicktimer);
//	var o = getEventSources();
//	switch(o.tagName) {
//		case "rect":
//			{
//				if(o.getAttribute("G")) {
//					templine.firstId = o.getAttribute("G");
//					var temp = document.getElementById(templine.firstId).getAttribute("transform");
//					console.log(temp);
//					if(temp.lastIndexOf("scale") > -1) {
//						temp = temp.substring(0, temp.lastIndexOf("scale"));
//					}
//					temp = temp.replace("translate(", "");
//					temp = temp.replace(")", "");
//					temp = temp.split(",");
//					if((parseInt(temp[0]) + 50)){
//						templine.firstPoint = (parseInt(temp[0]) + 50) + "," + (parseInt(temp[1]) + 25);
//					}else{
//						clearTempLine();
//					}
//					document.getElementById("svg").style.cursor = "crosshair";
//				}
//			}
//			break;
//	}
//}

/**
 *  全局的鼠标 按下事件
 * @param {Object} event
 */
document.onclick = function(event) {
	var o = getEventSources();
	console.log(o.tagName.toString());
	if(o.tagName.toString().localeCompare("svg") == 0) {
		return false;
	} else if(o.getAttribute("self") || o.tagName.toString().localeCompare("rect") == 0) {
		document.getElementById("svg").style.cursor = "wait";
	}
	clearTimeout(clicktimer);

	clicktimer = setTimeout(function() {
		console.log("--------------onclick----------------");

		selectionId = o.getAttribute("id");
		booleaniszoom = false;
		//图形业务事件

		//图形处理
		graph.init(o);

		//业务处理
		if(o.getAttribute("self")) {
			flow.init(o.getAttribute("self").toString(), o);
		}

		document.getElementById("svg").style.cursor = "default";
	}, 300);
	event.stopPropagation();
}

/**
 * 鼠标按下操作
 * @param {Object} event
 */
document.onmousedown = function(event) {
	var o = getEventSources();
	var tempTagName = o.tagName.toString();
	if(tempTagName.localeCompare("rect") == 0) {
		booleaniszoom = false;
		if(o.getAttribute("G")) {
			templine.firstId = o.getAttribute("G");
			var temp = document.getElementById(templine.firstId).getAttribute("transform");
			console.log(temp);
			if(temp.lastIndexOf("scale") > -1) {
				temp = temp.substring(0, temp.lastIndexOf("scale"));
			}
			temp = temp.replace("translate(", "");
			temp = temp.replace(")", "");
			temp = temp.split(",");
			if((parseInt(temp[0]) + 50)) {
				templine.firstPoint = (parseInt(temp[0]) + 50) + "," + (parseInt(temp[1]) + 25);
			} else {
				clearTempLine();
			}
			document.getElementById("svg").style.cursor = "crosshair";
		}
	} else if(tempTagName.localeCompare("svg") == 0) {
		booleaniszoom = true;
	}
}

/**
 * 全局的鼠标 移动事件
 * @param {Object} event
 */
document.onmousemove = function(event) {

}

/**
 * 全局的鼠标 抬起事件
 * @param {Object} event
 */
document.onmouseup = function(event) {
	booleaniszoom = false;
	var o = getEventSources();
	var tempTagName = o.tagName.toString();
	console.log("tempTagName",tempTagName);
	if(tempTagName.localeCompare("rect") == 0) {
		if(o.getAttribute("G")) {
			templine.endId = o.getAttribute("G").toString();
			g.setEdge(templine.firstId, templine.endId);
			_base._build();
			selectionId = "";
			pathLists.push({
				from: templine.firstId,
				to: templine.endId
			});
			each(propertyList, function(i, o) {
				if(o.key.toString().localeCompare(templine.firstId) == 0) {
					//开始点
					this.value.outlets++;
				} else if(o.key.toString().localeCompare(templine.endId) == 0) {
					//结束站
					this.value.inlets++;
				}
				return true;
			});
			console.log("propertyList-end", propertyList);
			clearTempLine();
			document.getElementById("svg").style.cursor = "default";
		}
	}else {
		clearTempLine();
	}
}