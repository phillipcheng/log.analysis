/**
 * 所有 事件功能的集合
 * 所有的事件,全部是公共事件,根据触发事件的元素和参数进行确定
 * (拖动事件除外)
 */

/**
 * 全局键盘事件
 * @param {Object} event
 */
document.onkeydown = function(event) {
	var e = event || window.event || arguments.callee.caller.arguments[0];
	if(e && e.keyCode == 27) { // 按 Esc 取消操作

	}

	if(e && e.keyCode == 47) { // 按 Delete 删除操作

	}

	if(e && e.keyCode == 76) { // 按  L或l 
		//drawFitstPoint();
	}
}

/**
 *  全局的鼠标 按下事件
 * @param {Object} event
 */
document.onmousedown = function(event){
	var o = getEventSources();
	selectionId = "";
	
	if(o.getAttribute("G")){
		selectionId = o.getAttribute("G").toString();
		_svg.clearSelectedRect(o);
		//选中的,选择红色的外边框
		
	}
	
	if(o.tagName.toString().localeCompare("svg")==0){
		booleaniszoom = true;
	}else{
		booleaniszoom = false;
	}
}

/**
 * 全局的鼠标 抬起事件
 * @param {Object} event
 */
document.onmouseup = function(event){
	booleaniszoom = false;
}

/**
 * 全局点击事件
 * @param {Object} event
 */
document.onclick = function(event) {
	event.stopPropagation();
	var o = getEventSources();
	var tagNameArgs = o.tagName.toString();

	switch(tagNameArgs) {

		case "svg":
			_svg.clearSelectedRect(o);	
			break;
			
		case "rect":
			_rect.init(o);
			break;
			
		case "circle":
				_circle.init(o);
			break;
			
		case "path":
				_path.init(o);
			break;
			
	}
}

/**
 * 全局双击事件 
 * 只是用来       画线      而使用的
 * @param {Object} event
 */
document.ondblclick = function(event) {

}
