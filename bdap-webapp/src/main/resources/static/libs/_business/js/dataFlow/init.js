var booleaniszoom = false; //zoom 操作

var selectionId = "";

var current_zoom_x = 0;

var current_zoom_y = 0;

var current_zoom_old_x = 0;

var current_zoom_old_y = 0;

var current_zoom_new_x = 0;

var current_zoom_new_y = 0;

var display_off_left = 0;

var display_off_top = 0;

/**
 * 用于记录临时点的线的集合
 */
var templine = {
	firstId: '',
	endId: '',
	firstPoint: '',
	endPoint: ''
};

/**
 * 节点列表
 * 
 */
var nodeLists = [];

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
		.on("zoom", zoom) // <-D
	).append("g").attr("id", "main");

	/**
	 * 1.2
	 */
	d3.select("#main")
		.append("path").attr("id", "pathmove").attr("d", "").attr("fill", "none")
		.attr("stroke", "#269ABC").attr("stroke-width", "2px")
		.attr("marker-end", "url(#arrow)");

	d3.select("#svg").attr("onmousemove", "svgMouseMove()");
	
	
	/**
	 * 1.3
	 */
	d3.select("#main").append("g").attr("id", "svg_path");
	
	/**
	 * 1.4
	 */
	d3.select("#main").append("g").attr("id", "svg_node");
}

