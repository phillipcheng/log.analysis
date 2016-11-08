
/**
 * 找到一个对象在屏幕上的 top位置
 * @param {Object} o
 */
var positionTop = function(o) {
	var offset = o.offsetTop;
	if(o.offsetParent) offset += arguments.callee(o.offsetParent);
	return offset;
}

/**
 * 找到一个对象在屏幕上的 left位置
 * @param {Object} o
 */
var positionLeft = function(o) {
	var offset = o.offsetLeft;
	if(o.offsetParent) offset += arguments.callee(o.offsetParent);
	return offset;
}


