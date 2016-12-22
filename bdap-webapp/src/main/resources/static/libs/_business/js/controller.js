var svg_document_onkeydown = function() {
	var e = window.event || arguments.callee.caller.arguments[0];
	console.log(e);

	if(e && e.keyCode == 46) { // 按 delete 取消操作
		clearTempLine();
		document.getElementById("svg").style.cursor = "default";
	}
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
				}, 1500);
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

var make_sure_first_point = function(o) {
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

var make_sure_second_point = function(o) {
	if(g_mouse_down.length > 0 && g_mouse_up.length > 0 && (g_mouse_down.localeCompare(g_mouse_up) != 0)) {
		var firstPointData = g.node(g_mouse_down);
		var endPointData = g.node(g_mouse_up);
		if(firstPointData.nodeType.localeCompare("end") == 0 ||
			endPointData.nodeType.localeCompare("start") == 0) {
			
		} else {
			g.setEdge(g_mouse_down, g_mouse_up);
			pathLists.push({
				'fromNodeName': g_mouse_down,
				'toNodeName': g_mouse_up,
				'linkType': 'success'
			});
			_base._build();
		}
	}
}

/**
 * 
 */
var svg_document_onmousedown = function() {
	g_mouse_down = "";
	g_mouse_up = "";
	booleaniszoom = false;
	e = window.event || arguments.callee.caller.arguments[0];
	var o = getEventSources(e);
	var args_self = "";
	var pointID = "";
	if(o.getAttribute("self")) {
		args_self = o.getAttribute("self").toString();
	}
	var args_tagName = o.tagName.toString();

	switch(args_tagName) {
		case "svg":
			{
				document.getElementById("svg").style.cursor = "move";
			}
			break;
	}

	if(o.getAttribute("G")) {
		g_mouse_down = o.getAttribute("G").toString();
	}
	if(o.getAttribute("id")) {
		pointID = o.getAttribute("id").toString();
	}
	console.log("---mouse_down:" + args_self + "," + args_tagName + "," + g_mouse_down + "---");
	switch(args_self) {
		case "RECT": //1.确定连线的第一个点
			{
				if(args_tagName.localeCompare("rect") == 0) {
					var nodeData = g.node(g_mouse_down);
					make_sure_first_point(o); //1.确定连线的第一个点
				}
			}
			break;
		case "LOG":
			{
				openLogWin();
			}
			break;
		case "ShowProperty":
			{
				zoom.ShowProperty(g_mouse_down);
				setTimeout(function() {
					var nodeData = g.node(g_mouse_down);
					loadProperty(g_mouse_down, nodeData);
					//清除其它选中的效果
					allchangeClassNameForRect("nodeGSelected", "nodeG");
					d3.select("#" + g_mouse_down).attr("class", "nodeG nodeGSelected");
				}, 1000);
			}
			break;
		case "HideProperty":
			{
				countProperty = 0;
				clearProperty(g_mouse_down);
				d3.select("#divrightup").style({
					"display": "none"
				});
				zoom.HideProperty(g_mouse_down);
			}
			break;
		case "addInLetsPoint":
			{
			debugger
				var nodeData = g.node(g_mouse_down);
				var node = d3.selectAll("#" + g_mouse_down);
				addInletsPoint(node, nodeData, g_mouse_down);
			}
			break;
		case "addOutLetsPoint":
			{	
				var nodeData = g.node(g_mouse_down);
				var node = d3.selectAll("#" + g_mouse_down);
				addOutletsPoint(node, nodeData, g_mouse_down);
			}
			break;
		case "inletsPoint":
			{	
			debugger
				var nodeData = g.node(g_mouse_down);
				var node = d3.selectAll("#" + g_mouse_down);
				
				var windowKey = g_mouse_down + '_' +pointID;
				if($("#"+windowKey).length > 0){
					var options = $("#"+windowKey).window("options");
					var closed = options.closed;
					if(closed) {
						$("#"+windowKey).window("open");
					}
				}else{
					dataWindowIDs.push(windowKey);
					dywindow.buildDataWindow(windowKey);
				}
	
			}
			break;
		case "outletsPoint":
			{	
				var nodeData = g.node(g_mouse_down);
				var node = d3.selectAll("#" + g_mouse_down);
				
				var windowKey = g_mouse_down + '_' +pointID;
				if($("#"+windowKey).length > 0){
					var options = $("#"+windowKey).window("options");
					var closed = options.closed;
					if(closed) {
						$("#"+windowKey).window("open");
					}
				}else{
					dataWindowIDs.push(windowKey);
					dywindow.buildDataWindow(windowKey);
				}
			}
			break;
		default:
			{
				console.log("--------onmousedown,default--------");
				booleaniszoom = true;
			}
			break;
	}
}

/**
 * 
 */
var svg_document_onmouseup = function() {
	var e = window.event || arguments.callee.caller.arguments[0];
	var o = getEventSources(e);
	var args_tagName = o.tagName.toString();
	var args_self = "";
	if(o.getAttribute("self")) {
		args_self = o.getAttribute("self").toString();
	}
	if(args_tagName.localeCompare("svg") != 0) {
		if(o.getAttribute("G")) {
			g_mouse_up = o.getAttribute("G").toString();
			console.log("---mouse_up:" + args_self + "," + args_tagName + "," + g_mouse_up + "---");
		}
		if(g_mouse_down.localeCompare(g_mouse_up) == 0) {
			//进行onclick操作,提出一个函数单独处理
			documentClickOperation(e, o, g_mouse_down, g_mouse_up, args_tagName, args_self);
		} else {
			switch(args_self) {
				case "RECT":
					{
						var nodeData = g.node(g_mouse_up);
						make_sure_second_point(o);
						clearTempLine();
					}
					break;
			}
		}
	} else {
		clearTempLine();
	}
	clearTempLine();
	document.getElementById("svg").style.cursor = "default";
	document.getElementById("divleftdatasetproperty").style.display = "none";
}

var menu = [{
	title: 'Delete this Node!',
	action: function(elm, d, i) {
		g.removeNode(d);
		_base._build();
		each(nodeLists, function(i, o) {
			if(o.k.toString().localeCompare(d) == 0) {
				nodeLists.splice(i, 1);
				return false;
			} else {
				return true;
			}
		});
	}
}];

var pathmenu = [{
	title: 'Delete this Path!',
	action: function(elm, d, i) {
		g.removeEdge(d.v, d.w);
		_base._build();
		each(pathLists, function(i, o) {
			if(o.fromNodeName.toString().localeCompare(d.v) == 0&&
				o.toNodeName.toString().localeCompare(d.w) == 0) {
				pathLists.splice(i, 1);
				return false;
			} else {
				return true;
			}
		});
	}
}];

/**
 * 这里面进行处理单击操作
 * @param {Object} e
 * @param {Object} o
 * @param {Object} g_mouse_down
 * @param {Object} g_mouse_up
 * @param {Object} args_tagName
 * @param {Object} args_self
 */
var documentClickOperation = function(e, o, g_mouse_down, g_mouse_up, args_tagName, args_self) {
	clearTempLine();
	if(args_tagName.localeCompare("rect") == 0 || args_tagName.localeCompare("text") == 0) { //点击   rect 或 text
		if(g_mouse_down.localeCompare("") != 0 && g_mouse_up.localeCompare("") != 0) { //确认操作内容
			if(document.getElementById(g_mouse_up).getAttribute("class").toString().indexOf("nodeGSelected") > -1) {
				beSureClassName(g_mouse_up);
			} else {
				removeAllSelectedClass();
				d3.select("#" + o.getAttribute("G")).attr("class", "nodeG nodeGSelected");
				//判断是否加载loadProperty
				if(d3.select("#" + g_mouse_up).select(".nodePropertyG")[0][0] === null) {} else {
					var nodeDate = g.node(g_mouse_up);
					loadProperty(g_mouse_up, nodeDate);
				}
			}
		}
	} else if(args_tagName.localeCompare("path") == 0) {
		if(args_self.localeCompare("RUN") == 0) { //点击进行操作
		} else {
			var tempPathG = g_mouse_up;
			if(document.getElementById(tempPathG).getAttribute("class").toString().localeCompare("edge") == 0) {
				document.getElementById(tempPathG).setAttribute("class", "edge edgeSelected");
				o.setAttribute("marker-end", "url(#arrowSelected)");
			} else if(args_self.localeCompare("addInLetsPoint") == 0) {
				
			} else if(args_self.localeCompare("addOutLetsPoint") == 0) {
				
			} else {
				document.getElementById(tempPathG).setAttribute("class", "edge");
				o.setAttribute("marker-end", "url(#arrow)");
			}
		}
	}
}