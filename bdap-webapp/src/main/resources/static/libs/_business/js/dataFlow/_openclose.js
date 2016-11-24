/**
 * 打开右边框操作
 */
var openOperationRight = function(txt) {
	console.log("-----------openOperationRight-------------");
	selectionId = txt || selectionId;
	var theId = selectionId.replace("g_", "rectDatalog_");
	var temp_transform = g("#" + theId)[0].getAttribute("transform");
	var new_temp_transform = temp_transform.replace("scale(0,0)","scale(1,1)");
	
	var other = theId.replace("rectDatalog_", "rectLog_");
	console.log(other);
	console.log(g("#"+other)[0].getAttribute("transform").toString());
	if(g("#"+other)[0].getAttribute("transform").toString().indexOf("translate(90,0)")>-1){
		new_temp_transform = new_temp_transform.replace("translate(0,0)","translate(250,0)");
	}else{
		new_temp_transform = new_temp_transform.replace("translate(0,0)","translate(90,0)");
	}
	
	d3.select("#"+theId).transition().duration(500)
	.attr("transform",new_temp_transform);
}

/**
 * 打开右边框操作
 * other
 */
var openOperationRightOther = function(txt){
	console.log("-----------openOperationRightOther-------------");
	selectionId = txt || selectionId;
	var theId = selectionId.replace("g_", "rectLog_");
	var temp_transform = g("#" + theId)[0].getAttribute("transform");
	var new_temp_transform = temp_transform.replace("scale(0,0)","scale(1,1)");
	
	var other = theId.replace("rectLog_", "rectDatalog_");
	if(g("#"+other)[0].getAttribute("transform").toString().indexOf("translate(90,0)")>-1){
		new_temp_transform = new_temp_transform.replace("translate(0,0)","translate(250,0)");
	}else{
		new_temp_transform = new_temp_transform.replace("translate(0,0)","translate(90,0)");
	}
	
	d3.select("#"+theId).transition().duration(500)
	.attr("transform",new_temp_transform);
}

/**
 * 关闭右边框操作
 * @param {Object} txt
 */
var closeOperationRight = function(txt){
	console.log("-----------closeOperationRight-------------");
	selectionId = txt || selectionId;
	var theId = selectionId.replace("g_", "rectDatalog_");
	
	d3.select("#"+theId).transition().duration(500)
	.attr("transform","translate(0,0)scale(0,0)");
	
	var other = theId.replace("rectDatalog_", "rectLog_");
	console.log(g("#"+other)[0].getAttribute("transform").toString());
	if(g("#"+other)[0].getAttribute("transform").toString().indexOf("translate(250,0)")>-1){
		var temp_transform = "";
		temp_transform = g("#" + other)[0].getAttribute("transform").toString();
		temp_transform = temp_transform.replace("translate(250,0)","translate(90,0)");
		
		d3.select("#"+other).transition().duration(500)
		.attr("transform",temp_transform);
	}	
}

/**
 * 关闭右边框操作
 * Other
 * @param {Object} txt
 */
var closeOperationRightOther = function(txt){
	console.log("-----------closeOperationRightOther-------------");
	selectionId = txt || selectionId;
	var theId = selectionId.replace("g_", "rectLog_");
		
	d3.select("#"+theId).transition().duration(500)
	.attr("transform","translate(0,0)scale(0,0)");
	
	var other = theId.replace("rectLog_", "rectDatalog_");
	console.log(g("#"+other)[0].getAttribute("transform").toString());
	if(g("#"+other)[0].getAttribute("transform").toString().indexOf("translate(250,0)")>-1){
		var temp_transform = "";
		temp_transform = g("#" + other)[0].getAttribute("transform").toString();
		temp_transform = temp_transform.replace("translate(250,0)","translate(90,0)");	
		
		d3.select("#"+other).transition().duration(500)
		.attr("transform",temp_transform);
	}
}

/**
 * 打开的操作
 */
var openOperation = function() {
	//记录选中的内容,并记录选中的内容的坐标的位置的变换
	var temp_transform = g("#" + selectionId)[0].getAttribute("transform");
	var new_temp_transform = temp_transform.replace("scale(1)", "scale(0,0)");
	var temp = new Date().getTime();
	var childId = "";
	//判断是否已经展开过
	var isExist = false;
	each(nodeLists, function() {
		if(this.id.toString().localeCompare(selectionId) == 0) {
			if(this.childId) {
				childId = this.childId.toString();
				isExist = true;
				return false;
			}
		}

	});
	console.log("isExist", isExist);
	if(isExist) {
		new_temp_transform = temp_transform.replace("scale(1,1)", "scale(0,0)");
		d3.select("#" + selectionId).transition().duration(500)
			.each("end", function() {
				d3.select("#" + childId).transition().duration(200)
					.attr("transform", g("#" + childId)[0].getAttribute("transform").replace("scale(0,0)", "scale(1,1)"));
			})
			.attr("transform", new_temp_transform);

	} else {
		//不存在
		d3.select("#svg_node").append("g")
			.attr("id", "g_" + temp)
			.attr("transform", temp_transform)
			.attr("type", "rect")
			.attr("args", "150,150");

		d3.select("#" + selectionId).transition().duration(500)
			.each("end", function() {
				d3.select("#g_" + temp).append("rect")
					.transition().duration(200)
					.attr("id", "rect_" + temp)
					.attr("self", "self")
					.attr("width", "150").attr("height", "150")
					.attr("rx", "5").attr("ry", "5")
					.style({
						"fill": "rgb(238, 238, 238)",
						"stroke": "red",
						"stroke-width": "3"
					});

				d3.select("#g_" + temp).append("circle")
					.attr("r", 8).attr("self", "close").attr("id", "circle_" + temp)
					.attr("cx", 150)
					.style({
						"fill": "#B96717"
					});

				d3.select("#g_" + temp).append("path")
					.attr("id", "path_" + temp).attr("d", "M145,0 L155,0")
					.attr("self", "close").attr("stroke", "#ffffff").attr("stroke-width", "3");

				d3.select("#g_" + temp).append("path")
					.attr("id", "pathRUN_" + temp)
					.attr("d", "M10,10 L10,30 L20,20 Z")
					.style({
						fill: "green"
					})

			})
			.attr("transform", new_temp_transform);

		//记录之前和之后的位置信息
		each(nodeLists, function() {
			if(this.id.toString().localeCompare(selectionId) == 0) {
				this.childId = "g_" + temp;
				this.position = [];
				this.position.push(temp_transform);
				this.position.push(new_temp_transform);
			} else {
				var _tempxy = getXYfromTranslate(this.id.toString());
				var _temp_x = parseInt(tempxy.split(",")[0]);
				var _temp_y = parseInt(tempxy.split(",")[1]);
			}
		});

	}

	//记录其它需要变换的 节点的集合
	var tempxy = getXYfromTranslate();
	var temp_x = parseInt(tempxy.split(",")[0]);
	var temp_y = parseInt(tempxy.split(",")[1]);
	temp_x += 150;
	temp_y += 150;

}

/**
 * 关闭的操作
 */
var closeOperation = function() {

	var temp_transform = g("#" + selectionId)[0].getAttribute("transform");
	var new_temp_transform = "";
	new_temp_transform = temp_transform.substring(0, temp_transform.indexOf("scale"));
	new_temp_transform = new_temp_transform + "scale(0,0)";
	console.log(temp_transform);
	console.log(new_temp_transform);
	console.log("----------------------");
	var parentId = "";
	each(nodeLists, function() {
		if(this.childId) {
			if(this.childId.toString().localeCompare(selectionId) == 0) {
				parentId = this.id.toString();
				d3.select("#" + selectionId).transition().duration(500)
					.each("end", function() {
						console.log(g("#" + parentId)[0]);
						d3.select("#" + parentId).transition().duration(200)
							.attr("transform", g("#" + parentId)[0].getAttribute("transform").toString().replace("scale(0,0)", "scale(1)"));
					})
					.attr("transform", new_temp_transform);
			}
		}
	});

}

/**
 * 添加 入和出 的节点操作
 * @param {Object} o
 */
var addInputAndOutput = function(o) {
	//
	each(nodeLists, function() {
		if(this.id.toString().localeCompare(o.firstId) == 0) {
			//input 
			this.inputs.push(o.id);
		} else if(this.id.toString().localeCompare(o.endId) == 0) {
			//output
			this.outputs.push(o.id);
		}
	});

	console.log(nodeLists);
}

/**
 * 画 入和出 的节点操作
 * @param {Object} actionId
 */
var drawInputAndOutput = function(txtId) {
	var aryinput = [];
	var aryoutput = [];
	var g_id = "";
	var r = 6;

	/**
	 * 清除节点
	 */
	each(nodeLists, function() {
		if(this.id.toString().localeCompare(txtId) == 0) {
			aryinput = this.inputs;
			aryoutput = this.outputs;
			var ary = document.getElementById(this.id.toString()).getElementsByTagName("path");
			for(var i = 0; i < ary.length; i++) {
				if(ary[i].getAttribute("self").localeCompare("inputs") == 0 ||
					ary[i].getAttribute("self").localeCompare("outputs") == 0) {
					var childo = ary[i];
					document.getElementById(this.id).removeChild(childo);
				}
			}
			return false;
		}
	});

	/**
	 * 入节点 添加
	 */
	each(aryoutput, function(i, o) {
		d3.select("#" + txtId)
			.append("circle")
			.attr("self", "inputs")
			.attr("id", "circleinputs_" + i)
			.attr("r", 6)
			.attr("cx", i * 15)
			.style({
				fill: "blue"
			});
	});

	/**
	 * 出节点 添加
	 */
	each(aryinput, function(i, o) {
		d3.select("#" + txtId)
			.append("circle")
			.attr("self", "outputs")
			.attr("id", "circleoutputs_" + i)
			.attr("r", 6)
			.attr("cx", i * 15)
			.attr("cy", 70)
			.style({
				fill: "red"
			});
	});
}