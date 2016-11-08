/**
 * 全局事件
 */
var init = function() {
	display = d3.select("#display");
	svg = d3.select("#displaysvg");

	var o = document.getElementById("display");
	display_off_left = positionLeft(o);
	display_off_top = positionTop(o);

	//******************  开始节点 初始化  ******************
	display.attr("onmousemove", "mouseMove()");

	display.append("div").attr("id", "node_star").style({
			left: "100px",
			top: "30px"
		}).text("star")
		.attr("onclick", "mouseNodeClick()")
		.call(d3.behavior.drag().on("drag", dragNodeMove))
		.append("div").attr("class", "point")
		.attr("onclick", "mouseNodePointClick()");

	//针对外部的内容进行事件初始化
	d3.select("#btn_one").attr("onmousedown", "mouseOuterActionDown()");
}

/**
 * 节点的画线点被点击事件
 * 这里用于纪录第一个点
 * @param {Object} result
 */
var nodeHadFirstLine = function(result) {

	event.stopPropagation();

	//纪录第一个点
	templine.firstPoint = (result.left - display_off_left) + "," + (result.top - display_off_top);
	templine.firstId = result.obj.parentNode.id;
	booleanmoveline = true;

}

/**
 * 节点被点击事件
 * 这里用于纪录第二个点
 * @param {Object} result
 */
var nodeHadEndLine = function(result) {

	//纪录第二个点
	if(result.obj.id.toString().localeCompare(templine.firstId) == 0) {
		console.log("return false");
		return false;
	} else if(templine.firstId.length > 0) {
		//画线吧

		//检查一下，是否进行了重复的画线操作
		if(!checkedExistLines(templine.firstId + "_" + result.obj.id)) {
			templine.endPoint = (event.x - display_off_left) + "," + (event.y - display_off_top);
			var temp_d = "M" + templine.firstPoint + " L" + templine.endPoint;

			if(action_move_y > 0) {
				temp_d = "M" + templine.firstPoint + " L";
				temp_d += (event.x - display_off_left) + ",";
				temp_d += action_move_y;
				templine.endPoint = (event.x - display_off_left) + "," + action_move_y;
			} else if(action_move_x > 0) {
				temp_d = "M" + templine.firstPoint + " L";
				temp_d += action_move_x + ",";
				temp_d += (event.y - display_off_top);
				templine.endPoint = action_move_x + "," + (event.y - display_off_top);
			}
			
			console.log("temp_d", temp_d);
			svg.append("path")
				.attr("id", templine.firstId + "_" + result.obj.id)
				.attr("d", temp_d).style({
					stroke: "#269ABC",
					"stroke-width": 2
				})
				.attr("marker-end", "url(#arrow)");

			//加到线的列表里面
			var temp_obj = {};
			temp_obj.id = templine.firstId + "_" + result.obj.id;
			temp_obj.firstId = templine.firstId;
			temp_obj.endId = result.obj.id;
			temp_obj.firstPoint = templine.firstPoint;
			temp_obj.endPoint = templine.endPoint;
			temp_obj.direction = action_move_direction;
			lines.push(temp_obj);
		}

		//恢复 初始化 移动化线功能
		booleanmoveline = false;
		initTempLine();
	}
}

/**
 * 节点移动 修改线的位置
 * @param {Object} objId
 */
var changeLines = function(objId) {
	var o = document.getElementById(objId);
	var ostyle = o.currentStyle ? o.currentStyle : window.getComputedStyle(o, null);
	if(templine.firstId.toString().localeCompare(objId) == 0) {
		//改变临时点的位置
		templine.firstPoint = (parseInt(ostyle.left) + (parseInt(ostyle.width) / 2)) + "," + (parseInt(ostyle.top) + parseInt(ostyle.height) + 5);
	} else {
		//console.log("lines",lines);
		for(var i = 0; i < lines.length; i++) {
			if(lines[i]["id"].indexOf(objId) > -1) {
				//console.log("find","------find------");
				if(lines[i]["firstId"].toString().localeCompare(objId) == 0) {
					//记录线的开始坐标
					//console.log("here","--------here---------");
					var temp_d = "M" + (parseInt(ostyle.left) + (parseInt(ostyle.width) / 2)) + "," + (parseInt(ostyle.top) + parseInt(ostyle.height) + 5);
					temp_d += " L";
					temp_d += lines[i]["endPoint"].toString();
					d3.select("#" + lines[i]["id"]).attr("d", temp_d);
					lines[i]["firstPoint"] = (parseInt(ostyle.left) + (parseInt(ostyle.width) / 2)) + "," + (parseInt(ostyle.top) + parseInt(ostyle.height) + 5);
				} else if(lines[i]["endId"].toString().localeCompare(objId) == 0) {
					//记录线的结束坐标
					console.log("there", "--------there---------");
					var temp_d = "M" + lines[i]["firstPoint"] + " L";
					if(lines[i]["direction"].toString().localeCompare("top")==0){
						temp_d += parseInt(ostyle.left) + parseInt(ostyle.width)/2;
						temp_d += ",";
						temp_d += parseInt(ostyle.top) - 10;
					}else if(lines[i]["direction"].toString().localeCompare("bottom")==0){
						temp_d += parseInt(ostyle.left) + parseInt(ostyle.width)/2;
						temp_d += ",";
						temp_d += parseInt(ostyle.top) + 10;
					}else if(lines[i]["direction"].toString().localeCompare("left")==0){
						temp_d += parseInt(ostyle.left) - 10;
						temp_d += ",";
						temp_d += parseInt(ostyle.top) + parseInt(ostyle.height)/2;
					}else if(lines[i]["direction"].toString().localeCompare("right")==0){
						temp_d += parseInt(ostyle.left) + 10;
						temp_d += ",";
						temp_d += parseInt(ostyle.top) + parseInt(ostyle.height)/2;
					}
					d3.select("#" + lines[i]["id"]).attr("d", temp_d);
					lines[i]["endPoint"] = (parseInt(ostyle.left) + (parseInt(ostyle.width) / 2)) + "," + (parseInt(ostyle.top) + parseInt(ostyle.height) + 5);
				}
			}
		}
	}
}

/**
 * 检查线是否已经存在了
 * 存在  return true , 不存在 return false
 * @param {Object} lineId
 */
var checkedExistLines = function(lineId) {
		var is_exist = false;
		for(var i = 0; i < lines.length; i++) {
			if(lines[i]["id"].toString().localeCompare(lineId) == 0) {
				//此线已经存在了
				is_exist = true;
				break;
			}
		}
		return is_exist;
	}
	/**
	 * 创建 一个 新的 节点 事件
	 */
var createAction = function() {
	var temp_id = "node_";
	temp_id += new Date().getTime();
	display.append("div")
		.attr("id", temp_id)
		.attr("class", "nodeAction")
		.style({
			left: (event.x - display_off_left - 50) + "px",
			top: (event.y - display_off_top - 50) + "px"
		})
		.text("Action")
		.attr("onclick", "mouseNodeClick()")
		.attr("onmousemove", "mouseActionMove()")
		.attr("onmouseout", "mouseActionOut()")
		.call(d3.behavior.drag().on("drag", dragNodeMove));

}