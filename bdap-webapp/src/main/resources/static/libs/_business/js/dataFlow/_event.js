/**
 * 全局键盘事件
 * @param {Object} event
 */
document.onkeydown = function(event) {
	var e = event || window.event || arguments.callee.caller.arguments[0];
	//console.log(e.keyCode);
	if(e && e.keyCode == 27) { // 按 Esc 取消操作

		clearTempLine();
		console.log("selectionId:", selectionId);
		each(linesLists, function(i, o) {
			if(o.id.toString().localeCompare(selectionId) == 0) {
				o.id = "";
				//节点中删除
				var parent_o = g("#svg_line")[0];
				var child_o = g("#" + selectionId)[0];

				parent_o.removeNode(child_o);

				return false;
			}
		});

		selectionId = "";
	}

	if(e && e.keyCode == 47) { // 按 Delete 删除操作

	}

	if(e && e.keyCode == 76) { // 按  L或l 
		//drawFitstPoint();
	}
}

/**
 * 鼠标 按下事件
 * @param {Object} event
 */
document.onmousedown = function(event) {
	event.stopPropagation();
	var o = getEventSources();
	//selectionId = o.getAttribute("id");
	//console.log(o);
	if(o.tagName.toString().localeCompare("svg") == 0) {
		//selectionId = "";

		var x = event.x;
		var y = event.y;

		x -= (display_off_left + current_zoom_x);
		y -= (display_off_top + current_zoom_y);

		drawMiddlePoint(x, y);

	} else if(o.tagName.toString().localeCompare("circle") == 0) {
		booleaniszoom = false;

		if(o.getAttribute("self")) {

		} else {
			//其它
			selectionId = o.getAttribute("id").toString();
			selectionId = "g_" + selectionId.substring(7);
			//1.画线
			drawFitstPoint();
		}
	} else if(o.tagName.toString().localeCompare("text") == 0) {

		booleaniszoom = false;
		selectionId = o.getAttribute("id").toString();
		selectionId = "g_" + selectionId.substring(5);

		drawFitstPoint();

	} else if(o.tagName.toString().localeCompare("path") == 0) {
		booleaniszoom = false;

		if(o.getAttribute("self")) {
			if(o.getAttribute("self").toString().localeCompare("Datalog") == 0) {
				//打开
				if(o.getAttribute("args").toString().localeCompare("open") == 0) {
					selectionId = o.getAttribute("id").toString();
					o.setAttribute("args", "close");
					d3.select("#" + selectionId).attr("args", "close");

					d3.select("#" + selectionId).attr("d", "M74,28 L86,28");
					selectionId = selectionId.replace("pathDatalog_", "g_");
					openOperationRight();
				} else if(o.getAttribute("args").toString().localeCompare("close") == 0) {
					selectionId = o.getAttribute("id").toString();
					o.setAttribute("args", "open");
					d3.select("#" + selectionId).attr("args", "open");

					d3.select("#" + selectionId).attr("d", "M74,28 L86,28 M80,23 L80,33");
					selectionId = selectionId.replace("pathDatalog_", "g_");
					closeOperationRight();
				}
			} else if(o.getAttribute("self").toString().localeCompare("Log") == 0) {
				if(o.getAttribute("args").toString().localeCompare("open") == 0) {
					selectionId = o.getAttribute("id").toString();
					o.setAttribute("args", "close");
					d3.select("#" + selectionId).attr("args", "close");

					d3.select("#" + selectionId).attr("d", "M74,48 L86,48");
					selectionId = selectionId.replace("pathLog_", "g_");
					openOperationRightOther();
				} else if(o.getAttribute("args").toString().localeCompare("close") == 0) {
					selectionId = o.getAttribute("id").toString();
					o.setAttribute("args", "open");
					d3.select("#" + selectionId).attr("args", "open");

					d3.select("#" + selectionId).attr("d", "M74,48 L86,48 M80,43 L80,53");
					selectionId = selectionId.replace("pathLog_", "g_");
					closeOperationRightOther();
				}
			} else if(o.getAttribute("self").toString().localeCompare("RUN") == 0) {
				//run 运行
				if(o.getAttribute("args").toString().localeCompare("play") == 0) {
					selectionId = o.getAttribute("id").toString();
					o.setAttribute("args", "pause");

					d3.select("#" + selectionId).attr("d", "M8,30 L8,50 L28,50 L28,30 Z")
						.style({
							fill: "red"
						});
				} else if(o.getAttribute("args").toString().localeCompare("pause") == 0) {
					selectionId = o.getAttribute("id").toString();
					o.setAttribute("args", "play");

					d3.select("#" + selectionId).attr("d", "M8,30 L8,50 L25,40 Z")
						.style({
							fill: "green"
						})
				}
			}
		} else {

		}

	} else if(o.tagName.toString().localeCompare("rect") == 0) {
		booleaniszoom = false;
		selectionId = o.getAttribute("id").toString();
		selectionId = selectionId.replace("rect_", "g_");

		if(o.getAttribute("self")) {
			selectionId = "g" + selectionId.substring(selectionId.indexOf("_"));
		} else {
			drawFitstPoint();
		}
	}

}

/**
 * 全局的鼠标双击事件
 */
//document.ondblclick = function(event) {
//	var o = getEventSources();
//	clearTempLine();
//
//	//var ischangeed = false;
//	//确定 选中 的元素是什么
//	if(o.tagName.toString().localeCompare("circle") == 0) {
//
//		booleaniszoom = false;
//		selectionId = o.getAttribute("id").toString();
//		selectionId = "g_" + selectionId.substring(7);
//		//ischangeed = true;
//	} else if(o.tagName.toString().localeCompare("text") == 0) {
//
//		booleaniszoom = false;
//		selectionId = o.getAttribute("id").toString();
//		selectionId = "g_" + selectionId.substring(5);
//		//ischangeed = true;
//	}else if(o.tagName.toString().localeCompare("rect") == 0) {
//
//		booleaniszoom = false;
//		selectionId = o.getAttribute("id").toString();
//		selectionId = "g_" + selectionId.substring(5);
//		//ischangeed = true;
//	}
//
//// 这里是新的变换 __************************开始**************************
//
//	if(ischangeed) {
////		console.log(selectionId);
////		console.log(g("#"+selectionId)[0]);
////		var temp = "";
////		
////		temp = g("#"+selectionId)[0].getAttribute("transform").toString();
////		temp = temp.replace("scale(1)","scale(1e-100)");
////		d3.select("#"+selectionId).transition().duration(200)
////		.attr("transform",temp);
////		
////		temp = new Date().getTime();
////		d3.select("#svg_node").append("g")
////		.attr("transform",g("#"+selectionId)[0].getAttribute("transform").toString())
////		.attr("id","g_"+temp)
////		.append("rect")
////		.attr("id","rect_"+temp)
////		.attr("width","150").attr("height","150")
////		.attr("rx","5").attr("ry","5")
////		.style({
////			"fill": "rgb(238, 238, 238)",
////			"stroke": "red",
////  		"stroke-width": "3"
////		});	
//	}
//
//
//// 这里是新的变换 __************************结束**************************
//
//
////	这里是旧的变换 __************************开始**************************
//
////	if(ischangeed) {  
////		//这里进行变换操作
////		each(nodeLists, function() {
////			if(this.id.toString().localeCompare(selectionId) == 0) {
////				//将选中的元素 进行放大操作
////				var tranformTxt = g("#" + selectionId)[0].getAttribute("transform");
////				if(tranformTxt.indexOf("scale(2)") > -1) {
////					tranformTxt = tranformTxt.replace("scale(2)", "scale(1)");
////					g("#" + selectionId)[0].setAttribute("transform", tranformTxt);
////				} else {
////					tranformTxt = tranformTxt.replace("scale(1)", "scale(2)");
////					tranformTxt = tranformTxt.replace("scale(0.5)", "scale(2)");
////					g("#" + selectionId)[0].setAttribute("transform", tranformTxt);
////					
////					changeNodeZoomPosition();//进行 与 Zoom 相关联的操作
////					
////				}
////
////			} else {
////				//将   没有    选中的元素   全部进行缩小操作
////				var tranformTxt = g("#" + this.id.toString())[0].getAttribute("transform");
////				tranformTxt = tranformTxt.replace("scale(1)", "scale(0.5)");
////				tranformTxt = tranformTxt.replace("scale(2)", "scale(0.5)");
////				g("#" + this.id.toString())[0].setAttribute("transform", tranformTxt);	
////			}
////		});
////	} else {
////		//在这里,进行还原操作
////		each(nodeLists, function() {
////			var tranformTxt = g("#" + this.id.toString())[0].getAttribute("transform");
////			tranformTxt = tranformTxt.replace("scale(0.5)", "scale(1)");
////			tranformTxt = tranformTxt.replace("scale(2)", "scale(1)");
////			g("#" + this.id.toString())[0].setAttribute("transform", tranformTxt);
////		});
////	}
//
////	这里是旧的变换 __************************结束**************************
//
//}

/**
 * 鼠标 抬起事件
 * @param {Object} event
 */
document.onmouseup = function(event) {
	selectionId = "";
	booleaniszoom = false;
}

/**
 * 鼠标在display上面进行  移动
 */
var displayMouseMove = function() {
	if(templine.endId.toString().localeCompare("") == 0) {
		drawMouseMoveLine();
	}

	if(selectionId.startsWith("path_")) {
		booleaniszoom = false;

		var x = event.x - display_off_left - current_zoom_x;
		var y = event.y - display_off_top - current_zoom_y;

		drawMiddlePoint(x, y);
	}
}

var testChange = function() {
	//	d3.select("#gchange").transition().duration(500)
	//		.each("end", function() {
	//			console.log("-----------finished-------------");
	//			d3.select("#gchangerect").attr("transform", "translate(250,200)scale(2)");
	//		})
	//		.attr("transform", "translate(600,200)scale(1)");

	//d3.select("#main").attr("transform", "translate(-256.35105103955186,-108.26800446718718)scale(1.6817928305074288)");

}

var changeZoom = function(txt) {
	if(txt == 0) {
		booleaniszoom = false;
	} else if(txt == 1) {
		booleaniszoom = true;
	}
}