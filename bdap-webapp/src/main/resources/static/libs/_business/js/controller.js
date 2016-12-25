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
	var e = window.event || arguments.callee.caller.arguments[0];
	var o = getEventSources(e);
	g_mouse_down = "";
	g_mouse_up = "";
	booleaniszoom = false;
	var args_self = "";
	var pointID = "";
	if(o.getAttribute("self")) {
		args_self = o.getAttribute("self").toString();
	}
	if(o.getAttribute("G")) {
		g_mouse_down = o.getAttribute("G").toString();
	}
	var args_tagName = o.tagName.toString();

	console.log("---mouse_down:" + args_self + "," + args_tagName + "," + g_mouse_down + "---");
	if(e.button == 0) { //左键
		switch(args_tagName) {
			case "svg":
				{
					clearTempLine();
					document.getElementById("svg").style.cursor = "move";
					document.getElementById("divdatanode").style.display = "none";
					document.getElementById("divleftdatasetproperty").style.display = "none";
				}
				break;
		}
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
			default:
				{
					console.log("--------onmousedown,default--------");
					booleaniszoom = true;
				}
				break;
		}
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
	if(o.getAttribute("G")) {
		g_mouse_up = o.getAttribute("G").toString();
		console.log("---mouse_up:" + args_self + "," + args_tagName + "," + g_mouse_up + "---");
	}

	if(g_mouse_down.localeCompare(g_mouse_up) == 0) {
		//进行onclick操作,提出一个函数单独处理
		documentClickOperation(e, o, g_mouse_down, g_mouse_up, args_tagName, args_self);
	}

	if(e.button == 0) { //左键
		if(args_tagName.localeCompare("svg") != 0) {
			switch(args_self) { //这里面,进行鼠标的抬起操作，鼠标的抬起操作，只是用于连线的操作
				case "RECT":
					{
						var nodeData = g.node(g_mouse_up);
						make_sure_second_point(o);
						clearTempLine();
					}
					break;
			}
		}
	}
	document.getElementById("svg").style.cursor = "default";
}

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
	if(e.button == 0) { //鼠标左键
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
			} else if(args_self.localeCompare("addInLetsPoint") == 0) {

			} else if(args_self.localeCompare("addOutLetsPoint") == 0) {

			} else {
				var tempPathG = g_mouse_up;
				if(document.getElementById(tempPathG).getAttribute("class").toString().localeCompare("edge") == 0) {
					document.getElementById(tempPathG).setAttribute("class", "edge edgeSelected");
					o.setAttribute("marker-end", "url(#arrowSelected)");
				} else {
					document.getElementById(tempPathG).setAttribute("class", "edge");
					o.setAttribute("marker-end", "url(#arrow)");
				}
			}
		} else if(args_tagName.localeCompare("circle") == 0) { //点击了圆的操作
			//这里进行样式的改变操作,
			if(args_self.localeCompare("inlets") == 0 || args_self.localeCompare("outlets") == 0) {
				//移除已经选中的样式
				//1.1
				d3.select("#rectContainer").selectAll(".inPath")
					.each(function() {
						var o = d3.select(this);
						var tempGId = o[0][0].getAttribute("G");
						var tempPId = o[0][0].id;
						if(checkedWeatherEdit(tempGId, tempPId)) {
							d3.select(this).attr("class", "inPath dataPathEdit");
						} else {
							d3.select(this).attr("class", "inPath");
						}
					});

				//1.2
				d3.select("#rectContainer").selectAll(".outPath")
					.each(function() {
						var o = d3.select(this);
						var tempGId = o[0][0].getAttribute("G");
						var tempPId = o[0][0].id;
						if(checkedWeatherEdit(tempGId, tempPId)) {
							d3.select(this).attr("class", "outPath dataPathEdit");
						} else {
							d3.select(this).attr("class", "outPath");
						}
					});

				//1.3
				//确定选中的样式
				if(args_self.localeCompare("inlets") == 0 || args_self.localeCompare("outlets") == 0) {
					var temp = o.getAttribute("class");
					temp = temp + " dataPathSelected";
					o.setAttribute("class", temp);
				}

				//1.4
				//加载数据绑定信息框
				var nodeId = g_mouse_down;
				var pathId = o.id;
				showDataBang(nodeId, pathId);
			}
		}
	} else if(e.button == 2) { //鼠标右键
		d3contextmenuShow();
	}
}

/**
 * 显示鼠标右键菜单
 */
var d3contextmenuShow = function() {
	var e = window.event || arguments.callee.caller.arguments[0];
	var o = getEventSources(e);
	var args_tagName = o.tagName;
	var args_self = o.getAttribute("self");

	switch(args_tagName) {
		case "rect":
			{ //删除节点
				document.getElementById("d3contextmenu").innerHTML = "<ul><li>Delete Node</li></ul>";
				document.getElementById("d3contextmenu").style.left = e.clientX + "px";
				document.getElementById("d3contextmenu").style.top = e.clientY + "px";
				document.getElementById("d3contextmenu").style.display = "block";
				document.getElementById("d3contextmenu").onclick = function() {
					var d = o.getAttribute("G");
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
					document.getElementById("d3contextmenu").style.left = "0px";
					document.getElementById("d3contextmenu").style.top = "0px";
					document.getElementById("d3contextmenu").style.display = "none";
				}
			}
			break;
		case "path":
			{
				document.getElementById("d3contextmenu").innerHTML = "<ul><li>Delete Path</li></ul>";
				document.getElementById("d3contextmenu").style.left = e.clientX + "px";
				document.getElementById("d3contextmenu").style.top = e.clientY + "px";
				document.getElementById("d3contextmenu").style.display = "block";
				document.getElementById("d3contextmenu").onclick = function() {
					var tempId = o.getAttribute("id");
					tempId = tempId.replace("pathA", "");
					tempId = tempId.split("A")
					g.removeEdge(tempId[0], tempId[1]);
					_base._build();
					each(pathLists, function(i, o) {
						if(o.fromNodeName.toString().localeCompare(tempId[0]) == 0 &&
							o.toNodeName.toString().localeCompare(tempId[1]) == 0) {
							pathLists.splice(i, 1);
							return false;
						} else {
							return true;
						}
					});
					document.getElementById("d3contextmenu").style.left = "0px";
					document.getElementById("d3contextmenu").style.top = "0px";
					document.getElementById("d3contextmenu").style.display = "none";
				}
			}
			break;
		case "circle":
			{
				if(args_self.localeCompare("inlets") == 0) {
					document.getElementById("d3contextmenu").innerHTML = "<ul><li>Delete Into Data Point</li></ul>";
					document.getElementById("d3contextmenu").style.left = e.clientX + "px";
					document.getElementById("d3contextmenu").style.top = e.clientY + "px";
					document.getElementById("d3contextmenu").style.display = "block";
					document.getElementById("d3contextmenu").onclick = function() {

						var tempG = o.getAttribute("G");
						var tempId = o.getAttribute("id");
						
						removeInletsPoint(tempG,tempId);

						document.getElementById("d3contextmenu").style.left = "0px";
						document.getElementById("d3contextmenu").style.top = "0px";
						document.getElementById("d3contextmenu").style.display = "none";
					}

				} else if(args_self.localeCompare("outlets") == 0) {
					document.getElementById("d3contextmenu").innerHTML = "<ul><li>Delete Data Point</li></ul>";
					document.getElementById("d3contextmenu").style.left = e.clientX + "px";
					document.getElementById("d3contextmenu").style.top = e.clientY + "px";
					document.getElementById("d3contextmenu").style.display = "block";
					document.getElementById("d3contextmenu").onclick = function() {
						
						var tempG = o.getAttribute("G");
						var tempId = o.getAttribute("id");						
						
						removeOutletsPoint(tempG,tempId);

						document.getElementById("d3contextmenu").style.left = "0px";
						document.getElementById("d3contextmenu").style.top = "0px";
						document.getElementById("d3contextmenu").style.display = "none";
					}
				}

			}
			break;
	}
}