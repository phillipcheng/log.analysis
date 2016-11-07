/**
 * 事件的集合
 */

/**
 * 全局鼠标事件
 * @param {Object} event
 */
document.onkeydown = function(event) {
	var e = event || window.event || arguments.callee.caller.arguments[0];
	console.log(e);
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
 */
var mouseMove = function() {
	var hander = event.srcElement ? event.srcElement : event.target;

	var result = {};
	result.obj = hander;
	result.left = event.x;
	result.top = event.y;

	if(booleanmoveline) {
		//根据鼠标的移动进行画线
		if(templine.middlePoint.toString().localeCompare("") == 0) {
			//只有两个点
			var temp_d = "M" + templine.firstPoint + " L" + (event.x - display_off_left) + "," + (event.y - display_off_top);
			d3.select("#linemove").attr("d", temp_d).style({
				stroke: "#269ABC",
				"stroke-width": 2
			});
		} else {
			
		}
	}

	//移动需要创建的节点
	if(booleanoutermove) {
		var o = document.getElementById("temp");
		o.style.left = (event.x - display_off_left - 50) + "px";
		o.style.top = (event.y - display_off_top - 50) + "px";
	}

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
	
	nodeHadEndLine(result);
	
	return result;
}

/**
 * 鼠标 开始画线点 被点击 事件
 */
var mouseNodePointClick = function() {
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