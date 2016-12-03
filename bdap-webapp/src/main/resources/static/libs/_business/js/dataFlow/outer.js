/**
 * 添加 新的 节点类型
 */

/**
 * 节点的类型集合
 */
var actionTypeLists = [];//节点的类型集合

/**
 * 节点的集合
 */
var actionLists = [];//节点的集合

/**
 * 检查 同一种类型,是否速重复
 * @param {Object} actionType
 * 
 * @return true=可以添加进去
 */
var checkInsertNodeType = function(actionType) {
	var isok = true;
	each(actionTypeLists, function() {
		if(this.toString().endsWith(actionType)) {
			isok = false;
			return false;
		}
	});
	return isok;
}

/**
 * 添加一个类型节点
 * @param {Object} actionType
 */
var addNewNodeType = function(actionType) {

	if(checkInsertNodeType(actionType)) {
		var o_accordion = d3.select("#accordion");

		var o_accordion_panel = o_accordion.append("div").attr("class", "panel panel-default");

		o_accordion_panel
			.append("div").attr("class", "panel-heading")
			.append("h4").attr("class", "panel-title")
			.append("a").attr("data-toggle", "collapse").attr("data-parent", "#accordion")
			.attr("href", "#collapseOne_" + actionType)
			.text(actionType); //panel-heading

		var temp_id = "node_" + actionType;
		o_accordion_panel
			.append("div").attr("id", "collapseOne_" + actionType).attr("class", "panel-collapse collapse")
			.append("div").attr("class", "panel-body").attr("id", temp_id);
		d3.select("#" + temp_id).append("button")
			.attr("type", "button")
			.attr("class", "btn btn-success").text("add")
			.attr("onclick", "addNewNodeForType('" + temp_id + "')");
		d3.select("#" + temp_id).append("ul").attr("class", "list-group")
			.attr("id", "ul_" + temp_id);

		actionTypeLists.push("collapseOne_" + actionType);
	}

}

/**
 * 添加一个新的类型实例
 * @param {Object} txt
 */
var addNewNodeForType = function(txt) {
	var parentId = txt;
	var o = {};
	o.id = "";
	o.text = "";
	var index = actionLists.length;
    index++;
    txt = txt.substring(5);
	d3.select("#"+parentId).append("li")
	.attr("id","action_"+txt+"_"+index)
	.attr("class","list-group-item").text(txt+"_"+index);
	
	addNode(2);
}
