/**
 * 画 鼠标 移动  的 线
 */
var drawMouseMoveLine = function() {

	if(templine.firstId.length > 0) {

		var x = event.x;
		var y = event.y;

		x -= (display_off_left + current_zoom_x);
		y -= (display_off_top + current_zoom_y);

		var temp_d = templine.firstPoint + " " + templine.middlePoint + " L" + x + "," + y;

		d3.select("#pathmove").attr("d", temp_d);
	}

}

/**
 * 清除 templine 的   属性
 */
var clearTempLine = function() {
	templine.firstId = "";
	templine.firstPoint = "";
	templine.middlePoint = "";
	templine.endPoint = "";
	templine.endId = "";

	d3.select("#pathmove").attr("d", "");
}

/**
 * 选中的第一个点 
 * 或者是 结束一条画线的操作
 */
var drawFitstPoint = function() {
	if(selectionId.length > 0) {
		if(templine.firstPoint.length == 0) { //还在没确定第一个点的前提下,画这第一个点
			templine.firstPoint = "M" + getXYfromTranslate();
			templine.firstId = selectionId;
			if(g("#" + selectionId)[0].getAttribute("type")) {
				if(g("#" + selectionId)[0].getAttribute("type").localeCompare("rect") == 0) {

					templine.firstPoint = templine.firstPoint.replace("M", "");

					var temp_x = parseInt(templine.firstPoint.split(",")[0]);
					var temp_y = parseInt(templine.firstPoint.split(",")[1]);
					var temp = g("#" + selectionId)[0].getAttribute("args").toString();

					temp_x += parseInt(temp.split(",")[0]) / 2;
					temp_y += parseInt(temp.split(",")[1]) / 2;

					templine.firstPoint = " M" + temp_x + "," + temp_y;
				}
			}

		} else if(templine.firstId.localeCompare(selectionId) != 0) {
			//确定画线了

			//检查一下,是否进行了重复的画线操作
			if(checkedLineExist(templine.firstId, selectionId)) {
				clearTempLine();
				return false;
			}

			templine.endId = selectionId;
			templine.endPoint = " L" + getXYfromTranslate();

			if(g("#" + selectionId)[0].getAttribute("type").localeCompare("rect") == 0) {

				templine.endPoint = templine.endPoint.replace(" L", "");

				var temp_x = parseInt(templine.endPoint.split(",")[0]);
				var temp_y = parseInt(templine.endPoint.split(",")[1]);
				var temp = g("#" + selectionId)[0].getAttribute("args").toString();

				temp_x += parseInt(temp.split(",")[0]) / 2;
				temp_y += parseInt(temp.split(",")[1]) / 2;

				templine.endPoint = " L" + temp_x + "," + temp_y;
			}

			var tempId = "path_" + templine.firstId + "_" + templine.endId;

			var o = {};
			o.id = tempId;
			o.firstId = templine.firstId;
			o.firstPoint = templine.firstPoint;
			o.middlePoint = templine.middlePoint;
			o.endPoint = templine.endPoint;
			o.endId = templine.endId;
			o.autoMiddle = 0;

			linesLists.push(o);

			d3.select("#svg_line").append("path")
				.attr("id", tempId)
				.attr("d", templine.firstPoint + templine.middlePoint + templine.endPoint)
				.attr("fill", "none").attr("stroke", "#269ABC").attr("stroke-width", "2px")
				.attr("marker-mid", "url(#arrow)")
				.attr("marker-start", "url(#arrow)")
				.attr("marker-end", "url(#arrow)")
				.attr("ondblclick", "doubleClickLine()");

			addInputAndOutput(o);
			drawInputAndOutput(o.firstId);
			drawInputAndOutput(o.endId);
			clearTempLine();
		}
	}
}

/**
 * 选中中间的节点
 */
var drawMiddlePoint = function(x, y) {

	if(templine.firstPoint.length > 0) {
		templine.middlePoint += " L" + x + "," + y + "";
	} else if(selectionId.startsWith("path_")) {
		each(linesLists, function() {
			if(this.id.toString().localeCompare(selectionId) == 0) {

				this.autoMiddle = 2;
				this.middlePoint = " S" + x + "," + y;
				this.endPoint = this.endPoint.replace("L", "");

				d3.select("#" + this.id)
					.attr("d", this.firstPoint + this.middlePoint + this.endPoint);

				return false;
			}
		});
	}
}

/**
 * 前提是对方选中了对象
 * 从translate中得到  X Y 的数值 
 * <g transform="
 * 	translate(157.01248168945312,129.60000610351562)scale(1)
 * " id="g_1479201341333"><circle r="30" id="circle_1479201341333" style="fill: green;"></circle><text x="-12" y="5" id="text_1479201341333" style="fill: white;">Star</text></g>
 */
var getXYfromTranslate = function(txt) {
	selectionId = txt || selectionId;
	if(selectionId.length > 0) {
		var temp = d3.select("#" + selectionId).attr("transform");
		if(temp.toString().length > 0) {
			temp = temp.replace("translate", "");
			if(temp.indexOf("scale") > -1) {
				temp = temp.substring(0, temp.indexOf("scale"));
			}
			temp = temp.replace("(", "");
			temp = temp.replace(")", "");
			return temp;
		}
	}
}

/**
 * 更改 , 改变 , 点的位置
 */
var changeLineDrapPosition = function(x, y) {
	if(selectionId.length > 0) {
		clearTempLine();
		//console.log(linesLists);
		each(linesLists, function() {
			if(this.firstId.toString().localeCompare(selectionId) == 0) {
				this.firstPoint = "M" + x + "," + y;
				d3.select("#" + this.id)
					.attr("d", this.firstPoint + this.middlePoint + this.endPoint);
			} else if(this.endId.toString().localeCompare(selectionId) == 0) {
				if(this.autoMiddle == 1 || this.autoMiddle == 0) {
					this.endPoint = " L" + x + "," + y;
				} else if(this.autoMiddle == 2) {
					this.endPoint = " " + x + "," + y;
				}
				d3.select("#" + this.id)
					.attr("d", this.firstPoint + this.middlePoint + this.endPoint);
			}
		});
	}
}

/**
 * 检查 连线, 是否重复
 */
var checkedLineExist = function(fitstTxt, endTxt) {
	var booleanisExist = false;
	each(linesLists, function() {
		if(this.firstId.toString().localeCompare(fitstTxt) == 0 &&
			this.endId.toString().localeCompare(endTxt) == 0) {
			booleanisExist = true;
			return false;
		}
	});
	return booleanisExist;
}

/**
 * 双击变成直线
 */
var doubleClickLine = function() {
	var pathId = getEventSources().getAttribute("id");

	each(linesLists, function() {
		if(this.id.toString().localeCompare(pathId) == 0) {
			var p_1 = this.firstPoint;
			var p_2 = this.endPoint;
			p_1 = p_1.replace("M", "");
			p_2 = p_2.replace(" L", "");

			var x1 = parseInt(p_1.split(",")[0]);
			var y1 = 0 - parseFloat(p_1.split(",")[1]);

			var x2 = parseInt(p_2.split(",")[0]);
			var y2 = 0 - parseFloat(p_2.split(",")[1]);

			var k = (y2 - y1) / (x2 - x1);
			var b = y1 - k * x1;

			var x3 = (x1 + x2) / 2;
			var y3 = k * x3 + b;

			y3 = y3 * (-1);

			this.middlePoint = " L" + x3 + "," + y3;
			this.autoMiddle = 1;

			d3.select("#" + this.id)
				.attr("d", this.firstPoint + this.middlePoint + this.endPoint);

			return false;
		}
	});

}

/**
 * 整体的坐标转换与平台操作
 */
var getMainTransformXAndY = function() {
	var temp = g("#main")[0].getAttribute("transform");
	if(temp.toString().length > 0) {
		temp = temp.replace("translate", "");
		temp = temp.substring(0, temp.indexOf("scale"));
		temp = temp.replace("(", "");
		temp = temp.replace(")", "");
		return temp;
	}
}