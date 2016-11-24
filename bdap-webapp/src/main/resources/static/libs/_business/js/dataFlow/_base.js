/**
 * #div : id = div
 * .div : class = div
 * _div : name = div
 * div : div
 * @param {Object} txt
 * @param {Object} results
 */
var g = function(txt, results) {
	results = results || [];
	if(txt.length > 0) {
		var ary = new Array();
		ary = txt.split(",");
		for(var i = 0; i < ary.length; i++) {
			var temp = ary[i].trim();
			if(temp.startsWith("#")) {
				temp = temp.substring(1);
				results = getById(temp, results);
			} else if(temp.startsWith(".")) {
				temp = temp.substring(1);
				results = getByClassNames(temp, results);
			} else if(temp.startsWith("_")) {
				temp = temp.substring(1);
				results = getByNames(temp, results);
			} else {
				results = getByTags(txt, results);
			}
		}
	} else {

	}
	return results;
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

//**********************************************************************

var getById = function(txt, results) {
	results = results || [];
	results.push(document.getElementById(txt));
	return results;
}

var getByTags = function(txt, results) {
	results = results || [];
	results.push.apply(results, document.getElementsByTagName(txt));
	return results;
}

var getByClassNames = function(txt, results) {
	results = results || [];
	results.push.apply(results, document.getElementsByClassName(txt));
	return results;
}
var getByNames = function(txt, results) {
	results = results || [];
	results.push.apply(results, document.getElementsByName(txt));
	return results;
}


/**
 * 得到事件源
 * @author huang peng
 */
var getEventSources = function(){
	return event.srcElement?event.srcElement:event.target;
}

/**
 * 获取屏幕左坐标
 */
var getOffsetLeft = function(o){
	var offset = o.offsetLeft;
	if(o.offsetParent){
		offset += arguments.callee(o.offsetParent);
	}
	return offset;
}

/**
 * 获取屏幕上坐标
 */
var getOffsetTop = function(o){
	var offset = o.offsetTop;
	if(o.offsetParent){
		offset += arguments.callee(o.offsetParent);
	}
	return offset;
}

/**
 * 通过ID , 获取当前的 Style
 */
var getStyleSourcesById = function(txt){
	//var o = g("#"+txt)[0];
	//var ostyle = o.currentStyle
}
