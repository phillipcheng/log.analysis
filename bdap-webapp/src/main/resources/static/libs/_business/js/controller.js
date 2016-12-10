/**
 *  全局的键盘 按下事件
 * @param {Object} event
 */
document.onkeydown = function(event) {
	var e = event || window.event || arguments.callee.caller.arguments[0];
	console.log(e);
	if(e && e.keyCode == 27) { // 按 Esc 取消操作
		clearTempLine();
	}

	if(e && e.keyCode == 46) { // 按 delete 取消操作
		var json = dagre.graphlib.json.write(g);
		console.info(json);
		//		d3.select("#rectContainer").selectAll(".rectNodeGSelected")
		//			.each(function() {
		//				var tempId = this.getAttribute("id").toString();
		//				g.removeNode(tempId); //node 节点
		//				each(pathLists, function(i, o) {
		//					if(o.dv.localeCompare(tempId) == 0 || o.dw.localeCompare(tempId) == 0) {
		//						console.log("------------------");
		//						console.log("1", i);
		//						pathLists.splice(i, 1);
		//						console.log("2", pathLists.length);
		//						console.log("3", --i);
		//						console.log(i);
		//						g.removeEdge(o.dv, o.dw);
		//						g.removeEdge(o.dw, o.dv);
		//						d3.select("#lineContainer").select("#linegroup" + o.dv + "A" + o.dw).remove();
		//						d3.select("#lineContainer").select("#linegroup" + o.dw + "A" + o.dv).remove();
		//						return i;
		//					}
		//				});
		//			}).remove();
		//
		//		d3.select("#lineContainer").selectAll(".edgeSelected").each(function() {
		//			var tempId = d3.select(this).attr("id");
		//			tempId = tempId.replace("linegroup", "");
		//			tempId = tempId.split("A");
		//			g.removeEdge(tempId[0], tempId[1]);
		//			each(pathLists, function(i, o) {
		//				if(o.dv.localeCompare(tempId[0]) == 0 && o.dw.localeCompare(tempId[1]) == 0) {
		//					pathLists.splice(i, 1);
		//					return false;
		//				}
		//			});
		//		}).remove();

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
document.ondblclick = function(event) {
	console.log("--------------ondblclick----------------");
	event.stopPropagation();
	clearTimeout(clicktimer);
	var o = getEventSources();
	switch(o.tagName) {
		case "rect":
			{
				if(o.getAttribute("G")) {
					templine.firstId = o.getAttribute("G");
					var temp = document.getElementById(templine.firstId).getAttribute("transform");
					if(temp.lastIndexOf("scale") > -1) {
						temp = temp.substring(0, temp.lastIndexOf("scale"));
					}	
					temp = temp.replace("translate(", "");
					temp = temp.replace(")", "");
					temp = temp.split(",");
					templine.firstPoint = temp[0] + "," + temp[1];
				}
			}
			break;
	}
	//	event.stopPropagation();
	//	var o = getEventSources();
	//	console.log(o);
	//	if(o.getAttribute("G") && o.tagName.toString().localeCompare("rect") == 0) {
	//		selectionId = o.getAttribute("G").toString();
	//		if(templine.firstId.length == 0) {
	//			//还没有点击第一个点
	//			templine.firstId = selectionId;
	//			templine.firstPoint = o.getAttribute("x") + "," + o.getAttribute("y");
	//		} else {
	//			//已经点击了第一个点，这个时候,确认第二个点
	//			if(selectionId.localeCompare(templine.firstId) != 0) {
	//				templine.endId = selectionId;
	//				pathLists.push({
	//					from: templine.firstId,
	//					to: templine.endId
	//				});
	//				g.setEdge(templine.firstId, templine.endId);
	//				_base._build();
	//				clearTempLine();
	//			}
	//		}
	//	}
}

/**
 *  全局的鼠标 按下事件
 * @param {Object} event
 */
document.onclick = function(event) {
	clearTimeout(clicktimer);
	var o = getEventSources();
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

	}, 300);
	event.stopPropagation();
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
}