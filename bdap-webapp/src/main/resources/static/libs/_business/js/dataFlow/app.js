var app = {
	txtId: "",
	init: function(txt) {
		this.txtId = txt || "g_" + new Date().getTime();
		d3.select("#svg_node").append("g").attr("id", this.txtId)
			.attr("transform", "translate(100,100)scale(1,1)")
			.call(d3.behavior.drag().on("drag", svgDrapMove));
		return this;
	},
	/**
	 * 
	 * @param {Object} type
	 * @param {Object} currentId
	 * @param {Object} parentId
	 * @param {Object} gId
	 */
	add: function(type, currentId, parentId, gId) {
		currentId = currentId || type + "_" + new Date().getTime();
		parentId = parentId || this.txtId;
		gId = gId || parentId;
		if(this.txtId) {
			var temp = new Date().getTime();
			d3.select("#" + parentId).append(type)
				.attr("G", gId).attr("id", currentId);
			this.txtId = currentId;
		}
		return this;
	},
	/**
	 * 
	 * @param {Object} type
	 * @param {Object} currentId
	 * @param {Object} gId
	 */
	append: function(type, currentId, gId) {
		currentId = currentId || type + "_" + new Date().getTime();
		gId = gId || this.txtId;
		if(this.txtId) {
			d3.select("#" + this.txtId).append(type)
				.attr("G", gId).attr("id", currentId);
			this.txtId = currentId;
		}
		return this;
	},
	find: function(txt) {
		this.txtId = txt;
		return this;
	},
	setA: function(data) {
		var o = d3.select("#" + this.txtId);
		for(var key in data) {
			d3.select("#" + this.txtId).attr(key, data[key]);
		}
		return this;
	},
	setTranslate:function(durations,keytxt,valtxt){
		d3.select("#" + this.txtId).transition().duration(durations).attr(keytxt,valtxt);
		return this;
	},
	setS: function(data) {
		d3.select("#" + this.txtId).style(data);
		return this;
	},
	txt: function(txt) {
		d3.select("#" + this.txtId).text(txt);
		return this;
	},
	innerH: function(txt) {
		d3.select("#" + this.txtId).innerHTML = txt;
		return this;
	}
}

var apprect = {
	init: function(o) {
		var _self = "";
		var _args = "";
		if(o.getAttribute("self")) {
			_self = o.getAttribute("self").toString();
		}
		if(o.getAttribute("args")) {
			_args = o.getAttribute("args").toString();
		}
	}
}

var appcircle = {
	init: function(o) {
		var _self = "";
		var _args = "";
		if(o.getAttribute("self")) {
			_self = o.getAttribute("self").toString();
		}
		if(o.getAttribute("args")) {
			_args = o.getAttribute("args").toString();
		}
	}
}

var apppath = {
	init: function(o) {
		var _self = "";
		var _args = "";
		if(o.getAttribute("self")) {
			_self = o.getAttribute("self").toString();
		}
		if(o.getAttribute("args")) {
			_args = o.getAttribute("args").toString();
		}
	}
}

/**
 * 
 * @param {Object} ary
 * @param {Object} fn
 */
var each = function(ary, fn) {
	for(var i = 0; i < ary.length; i++) {
		if(fn.call(ary[i], i, ary[i]) === false) {
			break;
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

		var temp = document.getElementById(selectionId).getAttribute("type").toString();
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
var svgMouseMove = function() {

	if(templine.firstId.length == 0) {

	} else {

		var x = event.x;
		var y = event.y;

		x -= (display_off_left + current_zoom_x);
		y -= (display_off_top + current_zoom_y);

		var temp_d = "M" + templine.firstPoint + " L" + x + "," + y;

		d3.select("#pathmove").attr("d", temp_d);
	}
}

/*
 * svg zoom 操作
 */
var zoom = function() {

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
var getEventSources = function() {
	return event.srcElement ? event.srcElement : event.target;
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