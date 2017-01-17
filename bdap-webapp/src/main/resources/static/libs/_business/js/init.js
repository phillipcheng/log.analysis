var clientwidth, clientheight, current_zoom_x, current_zoom_y,
	display_off_left, display_off_top, init_zoom_x, init_zoom_y, booleaniszoom;

var _HTTP_LOAD_ACTION_INFOR = "/dashview/george/flow/node/types/action/commands";
//var _HTTP_LOAD_ACTION_INFOR = "http://localhost:8020/flow/actionInfor";

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
 * 清除 templine 的   属性
 */
var clearTempLine = function() {

	templine.firstId = "";
	templine.firstPoint = "";

	templine.endPoint = "";
	templine.endId = "";

	d3.select("#pathmove").attr("d", "");
}

var remoteActionObj = {};
var remotePropertyObj = {};

var initPublicFunction = function(){
	String.prototype.replaceAll = function(oldCharts,newCharts){
		var txt = this;
		while(txt.indexOf(oldCharts)>-1){
			txt = txt.replace(oldCharts,newCharts);
		}
		return txt;
	}
};

var init = function() {
	actionLoadInit();
	initPublicFunction();
	console.info("WHOLE_PROJECT_ID: " + WHOLE_PROJECT_ID);
	//初始化位置的偏移
	clientwidth = document.body.clientWidth;
	clientheight = document.body.clientHeight;
	
	document.getElementById("child_small_svg").style.width = (clientwidth / 20) + "px";
	document.getElementById("child_small_svg").style.height = (clientheight / 20) + "px";

	document.getElementById("child_small_svg").style.left = (75 - (clientwidth / 40)) + "px";
	document.getElementById("child_small_svg").style.top = (75 - (clientheight / 40)) + "px";	

	current_zoom_x = clientwidth / 2 - 200;
	current_zoom_y = 50;

	var display = document.getElementById("home");
	display_off_left = getOffsetLeft(display);
	display_off_top = getOffsetTop(display);

	/**
	 * 1.1
	 */
	d3.select("#svg").call( // <-A
		d3.behavior.zoom() // <-B
		.scaleExtent([1, 1]) // <-C
		.on("zoom", svgzoom) // <-D
	).append("g").attr("id", "main");

	/**
	 * 1.2
	 */
	d3.select("#main").append("path").attr("id", "pathmove").attr("marker-end", "url(#arrow)")
		.attr("stroke", "#269ABC").attr("stroke-width", "2px");

	d3.select("#svg").attr("onmousemove", "svgMouseMove()");

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

	/**
	 * 1.6
	 */
	init_zoom_x = current_zoom_x;
	init_zoom_y = current_zoom_y;

	document.getElementById("child_svg").style.left = (clientwidth - 180) + "px";
	document.getElementById("child_svg").style.top = (clientheight - 210) + "px";

	/**
	 * 1.7
	 * loadJOSN
	 */
	var commandsURL = getAjaxAbsolutePath(_HTTP_LOAD_ACTION_INFOR) + "?projectId=" + WHOLE_PROJECT_ID;
	d3.json(commandsURL, function(data) {	
//	d3.json(_HTTP_LOAD_ACTION_INFOR, function(data) {
		remoteActionObj = data;
		$.each(data, function(k, v) {
			var temp = k;
			temp = temp.substring(temp.lastIndexOf(".") + 1);
			d3.select("#actionList")
				.append("li").append("a")
				.attr("href", "javascript:;").text(temp)
				.attr("onclick", "app.action({'label':'" + temp + "','cla':'" + k + "'})");
		});
	});
	remotePropertyObj = interact.getFlowSchema();
	console.info("remotePropertyObj: " + remotePropertyObj);
	each(remotePropertyObj.properties.nodes.items.jsonSchemas,function(){
			if(this.properties["cmd.class"]){
				propertyInfor.push(this.properties);
			}
			return true;
		});
	
	$.each(remotePropertyObj.properties.data.items.properties,function(k,v){
		if(v.type.localeCompare("string")==0&&v.enum){
			selfDataInfor[k] = {type:'string',v:v.enum};
		}else if(v.type.localeCompare('boolean')==0){
			selfDataInfor[k] = {type:'boolean'};
		}else if(v.type.localeCompare('string')==0){
			selfDataInfor[k] = {type:'string'};
		}
		return true;
	});	
	
	actionLoadManager();
	
	d3.select("#a_title").text(WHOLE_FLOW_NAME );
	result.name = WHOLE_FLOW_NAME;
	result.wfName = WHOLE_FLOW_NAME;
}

/**
 * 得到事件源
 * @author huang peng
 */
var getEventSources = function(e) {
	return e.srcElement || e.target;
}

/**
 * 
 * @param {Object} o
 */
var getOffsetLeft = function(o) {
	var offset = o.offsetLeft;
	if(o.offsetParent) {
		offset += arguments.callee(o.offsetParent);
	}
	return offset;
}

/**
 * 
 * @param {Object} o
 */
var getOffsetTop = function(o) {
	var offset = o.offsetTop;
	if(o.offsetParent) {
		offset += arguments.callee(o.offsetParent);
	}
	return offset;
}

/**
 * 得到 样式  源
 * @param {Object} o
 */
var getStyle = function(o) {
	return o.currentStyle || window.getComputedStyle(o, null);
}
var current_zoom_new_x = 0;
var current_zoom_new_y = 0;
var current_zoom_x = 0;
var current_zoom_y = 0;
var current_zoom_old_x = 0;
var current_zoom_old_y = 0;
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

		d3.select("#rectSmallContainer").attr("transform",
			"translate(" + (current_zoom_x / 20 + 75 - clientwidth / 40) + "," + (current_zoom_y / 20 + 75 - clientheight / 40) + ")scale(1,1)");

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
var svgMouseMove = function() {
	var e = window.event || arguments.callee.caller.arguments[0];
	if(templine.firstId.length == 0) {} else {
		try {
			var x = e.x || e.clientX;
			var y = e.y || e.clientY;
			x -= (display_off_left + current_zoom_x);
			y -= (display_off_top + current_zoom_y);
			var temp_d = "M" + templine.firstPoint + " L" + x + "," + y;
			d3.select("#pathmove").attr("d", temp_d);
		} catch(e) {
			clearTempLine();
		}
	}
}

/**
 * all browser conpatibility will be added here.
 */
var compatibilityTools = function() {
	if (typeof  String.prototype.endsWith  !=  'function')  {    
		String.prototype.endsWith  =   function(suffix)  {     
			return  this.indexOf(suffix,  this.length  -  suffix.length)  !==  -1;    
		};   
	}  

	if(typeof String.prototype.startsWith != 'function') {
		String.prototype.startsWith = function(prefix) {
			return this.slice(0, prefix.length) === prefix;
		};
	}
}

/**
 * 
 * @param {Object} relativePath
 */
var getAjaxAbsolutePath = function(relativePath) {
//	var httpPath = "http://" + WEB_IP + ":" + WEB_PORT;
	var httpPath = "";
	var userName = USER_NAME;
	if(relativePath != null && relativePath != '') {
		relativePath = relativePath.replace("{userName}", userName);
		httpPath += relativePath;
	} else {
		httpPath = "";
	}
	return httpPath;
}

var svg_onmousedown = function() {

}

var svg_onmouseup = function() {
	clearTempLine();
}

var isEmpty = function(str) {
	if(str == undefined || str == null || str == '') {
		return true;
	}
	return false;
}

/**
 * 移动到初始化的位置
 */
var remoeToInitPosition = function() {
	d3.select("#main").attr("transform", "translate(" + init_zoom_x + "," + init_zoom_y + ")scale(1,1)");
	d3.select("#rectSmallContainer").attr("transform", "translate(75,75)scale(1,1)");
	current_zoom_x = init_zoom_x;
	current_zoom_y = init_zoom_y;
}

var getRequestUrlParamString = function(name)
{
    var reg = new RegExp("(^|&)"+ name +"=([^&]*)(&|$)");
    var r = window.location.search.substr(1).match(reg);
    if(r!=null)return  unescape(r[2]); return null;
}

var actionLoadManager = function(){
	// view  add  edit
	var actionParam = getRequestUrlParamString("action");
	var typeParam = getRequestUrlParamString("type");
	var name = getRequestUrlParamString("name");
	if(actionParam == 'view' && typeParam =='flow'){
		WHOLE_FLOW_NAME = name;
		setTimeout(function(){
			FLOW_CURRENT_STAGE = "VIEW";
			load.loadBuild(name);
		}, 200);
		
	}else if(actionParam == 'edit' && typeParam =='flow'){
		WHOLE_FLOW_NAME = name;
		setTimeout(function(){
			FLOW_CURRENT_STAGE = "DESIGN";
			load.loadBuild(name);
		}, 200);
	} else if(actionParam == 'add' && typeParam =='flow'){
		var flowname = getRequestUrlParamString("flowname");
		var projectid = getRequestUrlParamString("projectid");
		WHOLE_FLOW_NAME = flowname;
		WHOLE_INSTANCE_ID = "";
		WHOLE_PROJECT_ID = projectid;
		
	}else if(actionParam == 'view' && typeParam =='job'){
		var flowname = getRequestUrlParamString("flowname");
		WHOLE_FLOW_NAME = flowname;
		WHOLE_INSTANCE_ID = name;
		FLOW_CURRENT_STAGE = "RUNNING";
		setTimeout(function(){
			FLOW_CURRENT_STAGE = "VIEW";
			load.loadBuild(flowname);
			setTimeout(function(){
				run.changeAllNodeColor();
			}, 1000);
		}, 200);
	}else if(actionParam == 'edit' && typeParam =='job'){
		
	}
//	var flowObj = interact.getFlow(WHOLE_FLOW_NAME);
//	WHOLE_PROJECT_ID = flowObj.projectId;
	
	return true;
}
/**
 * when page jump to flow page, init the relational parameters and const.
 */
var actionLoadInit = function(){
	// view  add  edit
	var actionParam = getRequestUrlParamString("action");
	var typeParam = getRequestUrlParamString("type");
	var name = getRequestUrlParamString("name");
	if(actionParam == 'view' && typeParam =='flow'){
		WHOLE_FLOW_NAME = name;
		FLOW_CURRENT_STAGE = "VIEW";
	}else if(actionParam == 'edit' && typeParam =='flow'){
		WHOLE_FLOW_NAME = name;
		FLOW_CURRENT_STAGE = "DESIGN";
	} else if(actionParam == 'add' && typeParam =='flow'){
		var flowname = getRequestUrlParamString("flowname");
		var projectid = getRequestUrlParamString("projectid");
		WHOLE_FLOW_NAME = flowname;
		WHOLE_INSTANCE_ID = "";
		WHOLE_PROJECT_ID = projectid;
		
	}else if(actionParam == 'view' && typeParam =='job'){
		var flowname = getRequestUrlParamString("flowname");
		WHOLE_FLOW_NAME = flowname;
		WHOLE_INSTANCE_ID = name;
		FLOW_CURRENT_STAGE = "VIEW";
	}else if(actionParam == 'edit' && typeParam =='job'){
		
	}
	if(isEmpty(WHOLE_PROJECT_ID)){
		var flowObj = interact.getFlow(WHOLE_FLOW_NAME);
		WHOLE_PROJECT_ID = flowObj.projectId;
	}
	return true;
}


//title msgString msgType: [error,info,question,warning]
var msgShow = function (title, msgString, msgType) {
	$.messager.alert(title, msgString, msgType);
}



