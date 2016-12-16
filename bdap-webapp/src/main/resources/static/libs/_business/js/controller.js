document.onkeydown = function() {
	var e = window.event || arguments.callee.caller.arguments[0];
	console.log(e);

	if(e && e.keyCode == 46) { // 按 delete 取消操作
		clearTempLine();
		document.getElementById("svg").style.cursor = "default";
	}
}

var node_mouse_down = function() {
	var e = window.event || arguments.callee.caller.arguments[0];
	e.stopPropagation();
	var o = getEventSources(e);
	console.log("-------------node_mouse_down:" + o.tagName + "-----------------");
	booleaniszoom = false;
	if(o.tagName.toString().localeCompare("rect") == 0) {
		if(o.getAttribute("G")) {
			templine.firstId = o.getAttribute("G");
			var temp = document.getElementById(templine.firstId).getAttribute("transform");
			if(temp.lastIndexOf("scale") > -1) {
				temp = temp.substring(0, temp.lastIndexOf("scale"));
			}
			temp = temp.replace("translate(", "");
			temp = temp.replace(")", "");
			temp = temp.split(",");
			if((parseInt(temp[0]) + 50) > 0) {
				templine.firstPoint = (parseInt(temp[0]) + 50) + "," + (parseInt(temp[1]) + 25);
			} else {
				//clearTempLine();
			}
			document.getElementById("svg").style.cursor = "crosshair";
		}
	}
}

var node_mouse_up = function() {
	var e = window.event || arguments.callee.caller.arguments[0];
	e.stopPropagation();
	var o = getEventSources(e);
	console.log("-------------node_mouse_up:" + o.tagName + "-----------------");
	if(o.tagName.toString().localeCompare("rect") == 0) {
		//画线
		if(o.getAttribute("G")) {
			if(o.getAttribute("G").toString().localeCompare(templine.firstId) == 0) {
				if(d3.select("#" + o.getAttribute("G")).attr("class").toString().localeCompare("nodeG nodeGSelected") == 0) {
					d3.select("#" + o.getAttribute("G")).attr("class", "nodeG");
				} else {
					//清除其它选中的效果
					allchangeClassNameForRect("nodeGSelected", "nodeG");
					d3.select("#" + o.getAttribute("G")).attr("class", "nodeG nodeGSelected");
					//如果,后来选中的那个,已经最大化,还要修改属性列表
					if(document.getElementById(o.getAttribute("G")).getElementsByTagName("g").length == 2) {
						var d = o.getAttribute("G");
						var nodeData = g.node(d);
						loadProperty(d, nodeData);
					}
				}
			} else {
				templine.endId = o.getAttribute("G").toString();
				g.setEdge(templine.firstId, templine.endId);
				_base._build();
				clearTempLine();
			}
		}
		clearTempLine();
		document.getElementById("svg").style.cursor = "default";
	}
}

var svg_mouse_down = function() {
	booleaniszoom = true;
}

var svg_mouse_up = function() {
	booleaniszoom = true;
	clearTempLine();
}

var zoom_click = function() {
	var e = window.event || arguments.callee.caller.arguments[0];
	e.stopPropagation();
	var o = getEventSources(e);
	console.log("-------------zoom_mouse_down:" + o.tagName + "-----------------");
	var args = o.getAttribute("class").toString();
	switch(args) {
		case "minNodePath":
		case "minNodeCircle":
		case "minNodeG":
			{
				var tempG = o.getAttribute("G").toString();
				zoom.ShowProperty(tempG);
				setTimeout(function() {
					var nodeData = g.node(nodeData);
					loadProperty(tempG, nodeData);
					//清除其它选中的效果
					allchangeClassNameForRect("nodeGSelected", "nodeG");
					d3.select("#" + o.getAttribute("G")).attr("class", "nodeG nodeGSelected");
				}, 1000);
			}
			break;
		case "maxNodePath":
		case "maxNodeCircle":
		case "maxNodeG":
			{
				countProperty = 0;
				var tempG = o.getAttribute("G").toString();
				clearProperty(tempG);
				zoom.HideProperty(tempG);
				allchangeClassNameForRect("nodeGSelected", "nodeG");
				d3.select("#" + o.getAttribute("G")).attr("class", "nodeG nodeGSelected");
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
	}
}

var log_click = function() {
	openLogWin();
}