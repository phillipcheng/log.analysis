/**
 * 事件的集合
 */

/**
 * 全局鼠标事件
 * @param {Object} event
 */
document.onkeydown = function(event) {
	var e = event || window.event || arguments.callee.caller.arguments[0];

	if(e && e.keyCode == 27) { // 按 Esc 取消操作
		//1.
		booleanmoveline = false;
		initTempLine();

		//2.
		document.getElementById("temp").style.display = "none";
		booleanoutermove = false;
	}
	if(e && e.keyCode == 47) { // 按 Delete 删除操作

	}
}

/**
 * 全局鼠标抬起事件
 */
document.onmouseup = function() {
	if(booleanoutermove) {
		booleanoutermove = false;
		var o = document.getElementById("temp");
		o.style.display = "none";

		createAction();
	}
}


/**
 * 鼠标移动事件
 * 用于在dispaly上面进行的动作
 */
var mouseMove = function() {

	var hander = event.srcElement ? event.srcElement : event.target;

	var result = {};
	result.obj = hander;
	result.left = event.x;
	result.top = event.y;
	console.log(booleanmoveline);
	if(booleanmoveline) {
		//根据鼠标的移动进行画线
		if(templine.endPoint.toString().localeCompare("") == 0) {
			//只有两个点
			var temp_d = "M" + templine.firstPoint + " L" + (event.x - display_off_left) + "," + (event.y - display_off_top);
			d3.select("#linemove").attr("d", temp_d).style({
				stroke: "#269ABC",
				"stroke-width": 2
			});
		}
	}

	//移动需要创建的节点
	//用于Action的操作
	if(booleanoutermove) {
		var o = document.getElementById("temp");
		o.style.left = (event.x - display_off_left - 50) + "px";
		o.style.top = (event.y - display_off_top - 50) + "px";
	}

}

/**
 * 鼠标移动事件
 * 用于在Action上面进行的动作
 * (内部移动)
 */
var mouseActionMove = function() {
	var hander = event.srcElement ? event.srcElement : event.target;

	var result = {};
	result.obj = hander;
	result.left = event.x;
	result.top = event.y;

	var ostyle = hander.currentStyle ? hander.currentStyle : window.getComputedStyle(hander, null);

	var x1 = parseInt(ostyle.left);
	var y1 = parseInt(ostyle.top);
	var x2 = x1 + parseInt(ostyle.width);
	var y2 = y1 + parseInt(ostyle.height);

	hander.style.borderColor = "#379082";
	action_move_direction = "";
	action_move_x = action_move_y = 0;

	if(Math.abs(x1 - (event.x - display_off_left)) <= 15) {
		hander.style.borderLeftColor = "red";
		action_move_x = parseInt(ostyle.left) - 10;
		action_move_direction = "left";
	} else if(Math.abs(x2 - (event.x - display_off_left)) <= 15) {
		hander.style.borderRightColor = "red";
		action_move_x = parseInt(ostyle.left) + parseInt(ostyle.width) + 10;
		action_move_direction = "right";
	} else if(Math.abs(y1 - (event.y - display_off_top)) <= 15) {
		hander.style.borderTopColor = "red";
		action_move_y = parseInt(ostyle.top) - 10;
		action_move_direction = "top";
	} else if(Math.abs(y2 - (event.y - display_off_top)) <= 15) {
		hander.style.borderBottomColor = "red";
		action_move_y = parseInt(ostyle.top) + parseInt(ostyle.height) + 10;
		action_move_direction = "bottom";
	} else {

	}

	return result;
}

/**
 * 鼠标移动事件
 * 用于在Action上面进行的动作
 * (外部移动)
 */
var mouseActionOut = function() {

	var hander = event.srcElement ? event.srcElement : event.target;
	hander.style.borderColor = "#379082";
	action_move_x = action_move_y = 0;
}

/**
 * 鼠标点击节点事件(js)
 */
var mouseNodeClick = function() {
	var hander = event.srcElement ? event.srcElement : event.target;
	
	var result = {};
	result.obj = hander;
	result.left = event.x;
	result.top = event.y;
	
	if(templine.firstId.toString().length == 0) {
		
	} else {
		nodeHadEndLine(result); //此方法用于记录 画线的第二个移点 而进行的处理		
	}

	return result;
}

/**
 * 鼠标 开始画线点 被点击 事件(star)
 */
var mouseNodePointClick = function() {
	event.stopPropagation();
	var hander = event.srcElement ? event.srcElement : event.target;
	var result = {};
	result.obj = hander;
	result.left = event.x;
	result.top = event.y;

	nodeHadFirstLine(result);
}

/**
 * 拖动事件(d3)
 */
var dragNodeMove = function() {

	this.parentNode.appendChild(this);
	var dragTarget = d3.select(this);

	dragTarget.style({
		left: d3.event.dx + parseInt(dragTarget.style("left")) + "px",
		top: d3.event.dy + parseInt(dragTarget.style("top")) + "px"
	});

	var result = {};

	result.obj = dragTarget;
	result.left = d3.event.dx + parseInt(dragTarget.style("left"));
	result.top = d3.event.dy + parseInt(dragTarget.style("top"));

	changeLines(dragTarget.attr("id"));

	event.stopPropagation();

	return result;
}