/**
 * 清除 templine 的   属性
 */
var clearTempLine = function() {

	templine.firstId = "";
	templine.firstPoint = "";

	templine.endPoint = "";
	templine.endId = "";

	d3.select("#pathmove").attr("d", "");
}

/**
 * * 全局初始化事件
 */
var init = function() {

	if (typeof  String.prototype.endsWith  !=  'function')  {    
		String.prototype.endsWith  =   function(suffix)  {     
			return  this.indexOf(suffix,  this.length  -  suffix.length)  !==  -1;    
		};   
	}  

	clientwidth = document.body.clientWidth;
	clientheight = document.body.clientHeight;

	current_zoom_x = clientwidth / 2 - 100;
	current_zoom_y = clientheight / 2 - 50;

	//初始化位置的偏移
	var display = document.getElementById("home");
	display_off_left = getOffsetLeft(display);
	display_off_top = getOffsetTop(display);

	/**
	 * 1.1
	 */
	d3.select("#svg").call( // <-A
		d3.behavior.zoom() // <-B
		.scaleExtent([1, 5]) // <-C
		.on("zoom", svgzoom) // <-D
	).append("g").attr("id", "main");

	/**
	 * 1.2
	 */
	d3.select("#main").append("path").attr("id", "pathmove").attr("marker-end", "url(#arrow)")
		.attr("stroke", "#269ABC").attr("stroke-width", "2px");

	d3.select("#svg").attr("onmousemove", "svgMouseMove()");
	d3.select("#svg").attr("onmousedown", "svg_mouse_down()");
	d3.select("#svg").attr("onmouseup", "svg_mouse_up()");

	/**
	 * 1.3
	 */
	d3.select("#main").append("g").attr("id", "rectContainer");

	/**
	 * 1.4
	 */
	d3.select("#main").append("g").attr("id", "lineContainer");

	/**
	 * 1.5
	 */
	d3.select("#main").attr("transform", "translate(" + current_zoom_x + "," + current_zoom_y + ")scale(1,1)");

}

/**
 * 得到 样式  源
 * @param {Object} o
 */
var getStyle = function(o) {
	return o.currentStyle || window.getComputedStyle(o, null);
}

/**
 * 得到事件源
 * @author huang peng
 */
var getEventSources = function(e) {
	return e.srcElement || e.target;
}

/**
 * 获取屏幕左坐标
 */
var getOffsetLeft = function(o) {
	var offset = o.offsetLeft;
	if(o.offsetParent) {
		offset += arguments.callee(o.offsetParent);
	}
	return offset;
}

/**
 * 获取屏幕上坐标
 */
var getOffsetTop = function(o) {
	var offset = o.offsetTop;
	if(o.offsetParent) {
		offset += arguments.callee(o.offsetParent);
	}
	return offset;
}

/*
 * svg zoom 操作
 */
var svgzoom = function() {
	if(booleaniszoom) {
		current_zoom_new_x = parseInt(d3.event.translate[0]);
		current_zoom_new_y = parseInt(d3.event.translate[1]);
		current_zoom_x += current_zoom_new_x - current_zoom_old_x;
		current_zoom_y += current_zoom_new_y - current_zoom_old_y;
		d3.select("#main").attr("transform",
			"translate(" + current_zoom_x + "," + current_zoom_y + ")scale(1,1)");
		current_zoom_old_x = current_zoom_new_x;
		current_zoom_old_y = current_zoom_new_y;
	} else {
		current_zoom_old_x = parseInt(d3.event.translate[0]);
		current_zoom_old_y = parseInt(d3.event.translate[1]);
	}
}

/**
 * 
 * @param {Object} ary
 * @param {Object} fn
 */
var each = function(ary, fn) {
	for(var i = 0; i < ary.length; i++) {
		var result = fn.call(ary[i], i, ary[i]);
		if(result === false) {
			break;
		} else if(result === true) {
			continue;
		} else {
			i = result;
		}
	}
}

/**
 * svg 的拖动事件
 */
var svgDrapMove = function() {
	booleaniszoom = false;
	var x = d3.event.x;
	var y = d3.event.y;

	if(selectionId.length > 0) {

		//changeLineDrapPosition(x, y);

		var temp = document.getElementById(selectionId).getAttribute("self").toString();
		if(temp.localeCompare("rect") == 0) {
			temp = document.getElementById(selectionId).getAttribute("args").toString();
			var temp_x = parseInt(temp.split(",")[0].toString());
			var temp_y = parseInt(temp.split(",")[1].toString());
			x -= (temp_x) / 2;
			y -= (temp_y) / 2;
		}

		d3.select("#" + selectionId).attr("transform",
			"translate(" + x + "," + y + ")scale(1,1)");

	}
}

/**
 * 鼠标移动 特效
 */
var svgMouseMove = function(event) {
	var e = event || window.event || arguments.callee.caller.arguments[0];
	if(templine.firstId.length == 0) {
	} else {
		var x = e.x || e.clientX;
		var y = e.y || e.clientY;
		console.log("x", x);
		console.log("y", y);
		x -= (display_off_left + current_zoom_x);
		y -= (display_off_top + current_zoom_y);
		var temp_d = "M" + templine.firstPoint + " L" + x + "," + y;
		console.log("temp_d", temp_d);
		d3.select("#pathmove").attr("d", temp_d);
	}
}

/**
 * 清除 templine 的   属性
 */
var clearTempLine = function() {

	templine.firstId = "";
	templine.firstPoint = "";

	templine.endPoint = "";
	templine.endId = "";

	d3.select("#pathmove").attr("d", "");
}