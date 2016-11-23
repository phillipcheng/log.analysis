/**
 * svg 中 加入 (star节点)
 */
var addStartNodeToSvg = function() {

	var tempId = new Date().getTime().toString();

	d3.select("#svg_node")
		.append("g")
		.attr("transform", "translate(100,100)scale(1)")
		.attr("id", "g_" + tempId)
		.attr("type", "circle")
		.attr("args", "30")
		.call(d3.behavior.drag().on("drag", svgDrapMove));

	d3.select("#g_" + tempId).append("circle").attr("r", 30).attr("id", "circle_" + tempId)
		.style({
			fill: "green"
		});

	d3.select("#g_" + tempId).append("text")
		.style({
			fill: "white"
		})
		.attr("x", "-12").attr("y", "5")
		.attr("id", "text_" + tempId)
		.text("Star");

	nodeLists.push({
		id: "g_" + tempId,
		type: 'circle'
	});

	var o = getEventSources();
	o.setAttribute("disabled", "disabled");

}

/**
 * svg 中 加入 (end节点)
 */
var addEndNodeToSvg = function() {

	var tempId = new Date().getTime().toString();

	d3.select("#svg_node")
		.append("g")
		.attr("transform", "translate(200,200)scale(1)")
		.attr("id", "g_" + tempId)
		.attr("type", "circle")
		.attr("args", "30")
		.call(d3.behavior.drag().on("drag", svgDrapMove));

	d3.select("#g_" + tempId).append("circle").attr("r", 30).attr("id", "circle_" + tempId)
		.style({
			fill: "red"
		});

	d3.select("#g_" + tempId).append("text")
		.style({
			fill: "white"
		})
		.attr("x", "-12").attr("y", "5")
		.attr("id", "text_" + tempId)
		.text("End");

	nodeLists.push({
		id: "g_" + tempId,
		type: 'circle'
	});

	var o = getEventSources();
	o.setAttribute("disabled", "disabled");
}

/**
 * 向列表中添加结点
 */
var addNodeToSvg = function() {

	var tempId = new Date().getTime().toString();

	// g
	d3.select("#svg_node")
		.append("g")
		.attr("transform", "translate(100,100)scale(1)")
		.attr("id", "g_" + tempId)
		.attr("type", "rect")
		.attr("args", "80,70")
		.call(d3.behavior.drag().on("drag", svgDrapMove));
	
	// g +  rect 
	d3.select("#g_" + tempId).append("rect")
		.attr("rx", "5").attr("ry", "5")
		.attr("id", "rect_" + tempId)
		.attr("width", "80").attr("height", "70")
		.style({
			fill: "rgb(238, 238, 238)",
			stroke: "rgb(166, 166, 166)"
		});
	
	//g + rect + run
	d3.select("#g_" + tempId).append("path")
		.attr("id", "pathRun_" + tempId)
		.attr("self","run")
		.attr("d", "M8,30 L8,50 L25,40 Z")
		.style({
			fill: "green"
		})
	
	// Datalog  openButton
	d3.select("#g_" + tempId).append("circle")
		.attr("r", 8)
		.attr("self", "Datalog")
		.attr("args","open")
		.attr("id", "circleDatalog_" + tempId)
		.attr("cx", 80)
		.attr("cy", 28)
		.style({
			"fill": "#B96717"
		});
		
	// Datalog  openButton + 
	d3.select("#g_" + tempId).append("path")
		.attr("id", "pathDatalog_" + tempId)
		.attr("d", "M74,28 L86,28 M80,23 L80,33")
		.attr("self", "Datalog")
		.attr("args","open")
		.attr("stroke", "#ffffff")
		.attr("stroke-width", "3");		
	
	// Log  openButton 
	d3.select("#g_" + tempId).append("circle")
		.attr("r", 8)
		.attr("self", "Log")
		.attr("args","open")
		.attr("id", "circleLog_" + tempId)
		.attr("cx", 80)
		.attr("cy", 48)
		.style({
			"fill": "#B96717"
		});		
	
    // Log  openButton +  
	d3.select("#g_" + tempId).append("path")
		.attr("id", "pathLog_" + tempId)
		.attr("d", "M74,48 L86,48 M80,43 L80,53")
		.attr("self", "Log")
		.attr("args","open")
		.attr("stroke", "#ffffff")
		.attr("stroke-width", "3");		
		
	
	d3.select("#g_" + tempId).append("text")
		.style({
			fill: "black",
			"font-size": "10pt"
		})
		.attr("x", "23").attr("y", "25")
		.attr("id", "text_" + tempId)
		.text("Action");
		
	// Data dialog
	d3.select("#g_" + tempId).append("rect")
	.attr("transform","translate(0,0)scale(0,0)")
	.attr("id", "rectDatalog_" + tempId)
	.attr("self", "self")
	.attr("args","open")
	.attr("width", "150").attr("height", "150")
	.attr("rx", "5").attr("ry", "5")
	.style({
		"fill": "rgb(238, 238, 238)",
		"stroke": "red",
		"stroke-width": "3"
	});
	
	// Lof dialog
	d3.select("#g_" + tempId).append("rect")
	.attr("transform","translate(0,0)scale(0,0)")
	.attr("id", "rectLog_" + tempId)
	.attr("self", "self")
	.attr("args","open")
	.attr("width", "150").attr("height", "150")
	.attr("rx", "5").attr("ry", "5")
	.style({
		"fill": "rgb(238, 238, 238)",
		"stroke": "orange",
		"stroke-width": "3"
	});	

	nodeLists.push({
		id: "g_" + tempId,
		type: 'rect',
		inputs:[],
		outputs:[],
		childId:[],
		positions:[]
	});

}

/**
 * 初始化事件
 */
var init = function() {
	//实初化控件
	display = g("#home")[0];
	displayLog = g("#divlog")[0];

	//初始化位置的偏移
	display_off_left = getOffsetLeft(display);
	display_off_top = getOffsetTop(display);

	//绑定事件
	d3.select("#divlog").call(d3.behavior.drag().on("drag", divDrapMove));
	displayLog.style.display = "none";

	d3.select("#svg").call( // <-A
		d3.behavior.zoom() // <-B
		.scaleExtent([1, 5]) // <-C
		.on("zoom", zoom) // <-D
	).append("g").attr("id", "main");

	d3.select("#main")
		.append("g").attr("id", "svg_line")
		.append("path").attr("id", "pathmove").attr("d", "").attr("fill", "none")
		.attr("stroke", "#269ABC").attr("stroke-width", "2px")
		.attr("marker-mid", "url(#arrow)").attr("marker-start", "url(#arrow)")
		.attr("marker-end", "url(#arrow)");
	//	<path id="pathmove" d="" fill="none" stroke="#269ABC" 
	//stroke-width="2px" marker-mid="url(#arrow)" 
	//marker-start="url(#arrow)" marker-end="url(#arrow)"></path>

	d3.select("#main").append("g").attr("id", "svg_node");

	d3.select("#svg").attr("onmousemove", "displayMouseMove()");

}

/**
 * 用于记录div的拖动效果
 */
var divDrapMove = function() {
	booleaniszoom = false;
	booleanchangezoom = true;
	this.parentNode.appendChild(this);
	var dragTarget = d3.select(this);

	dragTarget.style({
		left: d3.event.dx + parseInt(dragTarget.style("left")) + "px",
		top: d3.event.dy + parseInt(dragTarget.style("top")) + "px"
	});
}

/**
 * svg 的拖动事件
 */
var svgDrapMove = function() {
	booleaniszoom = false;
	booleanchangezoom = true;
	var x = d3.event.x;
	var y = d3.event.y;

	if(selectionId.length > 0) {

		changeLineDrapPosition(x, y);

		var temp = g("#" + selectionId)[0].getAttribute("type").toString();
		if(temp.localeCompare("rect") == 0) {

			temp = g("#" + selectionId)[0].getAttribute("args").toString();
			var temp_x = parseInt(temp.split(",")[0].toString());
			var temp_y = parseInt(temp.split(",")[1].toString());
			x -= (temp_x) / 2;
			y -= (temp_y) / 2;
		}

		d3.select("#" + selectionId).attr("transform",
			"translate(" + x + "," + y + ")scale(1)");

		booleanisDrap = true;
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
			"translate(" + current_zoom_x + "," + current_zoom_y + ")scale(1)");

		current_zoom_old_x = current_zoom_new_x;
		current_zoom_old_y = current_zoom_new_y;
		//booleaniszoom = false;
	} else {
		current_zoom_old_x = parseInt(d3.event.translate[0]);
		current_zoom_old_y = parseInt(d3.event.translate[1]);
		booleaniszoom = true;
	}

}

/**
 * 当产生  Zoom操作的时候,所产生的位置的变换操作
 */
var changeNodeZoomPosition = function() {
	var temp = getXYfromTranslate();
	var temp_x = parseInt(temp.split(",")[0]);
	var temp_y = parseInt(temp.split(",")[1]);
	var tempCurrentNode = selectionId;

	each(linesLists, function() {

		d3.select("#" + this.id.toString()).transition().duration(500)
			.attr("d", "M0,0 L" + temp)
			.attr("stroke-width", "0px");

	});

	each(nodeLists, function() {

		if(this.id.toString().localeCompare(tempCurrentNode) != 0) {

			this.positionx = temp_x;
			this.positiony = temp_y;

			d3.select("#" + this.id.toString()).transition().duration(1000)
				.attr("transform", "translate(" + this.positionx + "," + this.positiony + ")scale(1e-100)");
		} else {

		}

	});

}