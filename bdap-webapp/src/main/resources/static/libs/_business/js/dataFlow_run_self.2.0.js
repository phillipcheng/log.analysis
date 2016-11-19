/**
 * 拖动特效
 */
var dragNodeMove = function() {

	this.parentNode.appendChild(this);
	var dragTarget = d3.select(this);

	dragTarget.style({
		left: d3.event.dx + parseInt(dragTarget.style("left")) + "px",
		top: d3.event.dy + parseInt(dragTarget.style("top")) + "px"
	});

}

/**
 * 初始化 页面的内容
 */
var initDraw = function() {

	// star
	display.append("div").attr("id", "node_star").style({
			left: starJSON.left,
			top: starJSON.top
		}).text("star")
		.append("div").attr("class", "point");

	//actions
	for(var i = 0; i < actionsJSON.length; i++) {
		display.append("div")
			.attr("id", "temp_id")
			.attr("class", "nodeAction")
			.style({
				left: actionsJSON[i]["left"],
				top: actionsJSON[i]["top"]
			})
			.text("Action")
			.append("div")
			.attr("class", "pointNode");
	}

	//lines
	for(var i = 0; i < linesJSON.length; i++) {
		var temp_d = "M";
		temp_d += linesJSON[i]["firstPoint"];
		temp_d += " L";
		temp_d += linesJSON[i]["endPoint"];
		svg.append("path")
			.attr("d", temp_d).style({
				stroke: "#269ABC",
				"stroke-width": 2
			})
			.attr("marker-end", "url(#arrow)");
	}
}

/**
 * 软件功能的初始化
 */
var init = function() {

	display = d3.select("#display");
	svg = d3.select("#displaysvg");

	d3.select("#divoperation")
		.call(d3.behavior.drag().on("drag", dragNodeMove));

	initDraw();
}