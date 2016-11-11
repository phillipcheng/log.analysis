var display = null;
var svg = null;

var booleanCheckedBegin = false;

var x1 = y1 = x2 = y2 = 0;
var clickObj_One_id = "";
var clickObj_Two_id = "";

/**
 * 
 */
var _scopeData = {
	start: {
		x: 100,
		y: 100,
		from:[],
		to:[]
	},
	end: {
		x: 100,
		y: 500,
		from:[],
		to:[]
	},
	lines: []
};

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

/**
 * 鼠标点击
 */
var mouseclick = function() {
	var handle = event.srcElement ? event.srcElement : event.target;

	var temp_x = event.x - positionLeft(document.getElementById("display"));
	var temp_y = event.y - positionTop(document.getElementById("display"));

	if(handle.id.toString().indexOf("point") > -1) {
		//点击的位置是  point

		if(booleanCheckedBegin) {

			/**
			 * 完成画线的功能
			 * 1.判断,该线条是否已经重复产生
			 * 2.画线
			 * 3.保存画线数据
			 */

			clickObj_Two_id = handle.parentNode.id;
			
			//如果画线的点,是同一个点的时候,也不进行画线操作
			

			var tempId = "line_" + clickObj_One_id + "_" + clickObj_Two_id;

			if(_scopeData.lines.length > 0) {
				for(var i = 0; i < _scopeData.lines.length; i++) {
					if(_scopeData.lines[i].id.toString().localeCompare(tempId) == 0) {
						x1 = y1 = x2 = y2 = 0;
			 	  		clickObj_One_id = clickObj_Two_id = "";
			 	  		booleanCheckedBegin = false;
			 	  		d3.select("#linemove").attr("d", "M0,0");
						return false;
					}
				}
			}

			var tempObject = {
				id: tempId,
				from:'',
				to:'',
				points: []
			};

			x2 = event.x - positionLeft(document.getElementById("display"));
			y2 = event.y - positionTop(document.getElementById("display"));

			//调整一下箭头的点的位置
			if(handle.id.toString().indexOf("UP") > -1) {
				y2 -= 10;
			} else if(handle.id.toString().indexOf("DOWN") > -1) {
				y2 += 10;
			} else if(handle.id.toString().indexOf("LEFT") > -1) {
				x2 -= 10;
			} else if(handle.id.toString().indexOf("RIGHT") > -1) {
				x2 += 10;
			}
			
			
			//点加入到了JSON里面
			tempObject.from = clickObj_One_id;
			tempObject.to = clickObj_Two_id;
			tempObject.points.push(x1, y1, x2, y2);
			_scopeData.lines.push(tempObject);

			//画线
			var tempstring = "M" + x1 + "," + y1 + " L" + x2 + "," + y2;
			svg.append("path")
				.attr("id", tempId)
				.attr("d", tempstring)
				.attr("marker-end", "url(#arrow)")
				.style({
					stroke: "#269ABC",
					"stroke-width": 2
				})
				.call(d3.behavior.drag().on("drag", dragmoveForLine));

			clickObj_One_id = clickObj_Two_id = "";
			x1 = y1 = x2 = y2 = 0;

			d3.select("#linemove").attr("d", "M0,0");

			booleanCheckedBegin = false;

		} else {

			//开始画第一个点
			x1 = y1 = x2 = y2 = 0;
			clickObj_One_id = clickObj_Two_id = "";

			x1 = event.x - positionLeft(document.getElementById("display"));
			y1 = event.y - positionTop(document.getElementById("display"));

			booleanCheckedBegin = true;

			// 在这里 ， 记录第一个点击事件的 Object Id

			clickObj_One_id = handle.parentNode.id;
		}

	}

}

/**
 * 鼠标移动事件
 */
var mousemove = function() {
	var temp_x = event.x - positionLeft(document.getElementById("display"));
	var temp_y = event.y - positionTop(document.getElementById("display"));

	if(booleanCheckedBegin) {
		//确定了第一个点的时候,以第一个点为中心,移动画线
		var temp_d = "M" + x1 + "," + y1 + " L" + temp_x + "," + temp_y;

		d3.select("#linemove").attr("d", temp_d).style({
			stroke: "#269ABC",
			"stroke-width": 2
		});
	}

}

/**
 * 拖动line事件
 */
var dragmoveForLine = function() {
//	var temp_x = event.x - positionLeft(document.getElementById("display"));
//	var temp_y = event.y - positionTop(document.getElementById("display"));
//	document.getElementById("divmovepoint").style.left = temp_x + "px";
//	document.getElementById("divmovepoint").style.top = temp_y + "px";
}

/**
 * 拖动移动
 */
var dragmove = function() {
	this.parentNode.appendChild(this);
	var dragTarget = d3.select(this);
	dragTarget.style({
		left: d3.event.dx + parseInt(dragTarget.style("left")) + "px",
		top: d3.event.dy + parseInt(dragTarget.style("top")) + "px"
	});
}

/**
 * 全局的鼠标抬起事件
 * 
 */
var displaymouseup = function(){
	
}

/**
 * 
 */
var init = function() {
	display = d3.select("#display");
	svg = d3.select("#displaysvg");

	//**************************************移动初始化*************************
	display.attr("onmousemove", "mousemove()");

	//******************  开始节点 初始化  ******************
	display.append("div").attr("id", "star").style({
			left: "100px",
			top: "30px"
		}).call(d3.behavior.drag().on("drag", dragmove)).text("start")
		.append("div").attr("id", "starpointDOWN");

	//****************** 结束节点  初始化  ******************
	display.append("div").attr("id", "end").style({
			left: "100px",
			top: "500px"
		}).call(d3.behavior.drag().on("drag", dragmove))
		.text("end")
		.append("div").attr("id", "endpointUP");

	//**************************************节点点事件初始化*************************
	display.attr("onclick", "mouseclick()"); //鼠标点击事件

}

