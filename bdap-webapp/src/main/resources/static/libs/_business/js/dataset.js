var _dataset = {
	nodes: null,
	_build: function() {
		dagre.layout(g_dataSet);
		nodes = g_dataSet.nodes();
		nodes = this._sortNodes(g_dataSet);

		this._groupNode(g_dataSet);
	},
	_sortNodes: function(g) {
		var newNodes = new Array();
		$.each(nodes, function(i, d) {
			//从当前节点取到所有父级节点（包含本节点）
			while($.inArray(d, newNodes) == -1) {
				_getTopParent(d, newNodes);
			}
		});
		return newNodes;

		function _getTopParent(name) {
			var arr = newNodes;
			if($.inArray(name, arr) != -1) {
				return "";
			}
			var parent = g.parent(name);
			if($.inArray(parent, arr) != -1) {
				newNodes.push(name);
				return name;
			}
			if(parent != undefined) {
				//递归
				_getTopParent(parent);
			} else {
				newNodes.push(name);
				return name;
			}
		}
	},
	/**
	 * 用于节点
	 */
	_groupNode: function(g) {
		var groupNode = d3.select("#rectDataSetContainer").selectAll(".dataSetG");
		var group = groupNode.data(nodes, function(d) {
			return d;
		});
		group.enter().append("g") //enter
			.each(function(d) {
				var nodeData = g_dataSet.node(d);
				dataSetEnter(this, d, nodeData);
			});
		group.each(function(d) { //update
			var nodeData = g_dataSet.node(d);
			dataSetUpdate(this, d, nodeData);
		});
		group.exit()
			.each(function(d) {
				d3.select(this).remove();
				console.info("remove node: " + d);
			}).remove();
	}
}

/**
 * 
 * @param {Object} theSelf
 * @param {Object} d
 * @param {Object} nodeData
 */
var dataSetEnter = function(theSelf, d, nodeData) {
	var theSelfObj = d3.select(theSelf);
	theSelfObj.attr("class", "dataSetG").attr("id", d)
		.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)")
		.attr("onclick", "dataSet_Click('dataSetText" + d + "','" + d + "')");

	theSelfObj.append("rect")
		.attr("rx", 10).attr("ry", 10)
		.attr("G", d).attr("self", "INDATA")
		.attr("width", nodeData.width)
		.attr("height", nodeData.height);

	theSelfObj.append("text")
		.attr("id", "dataSetText" + d)
		.attr("x", 2)
		.attr("y", 20).text(nodeData.label);

}

/**
 * 
 * @param {Object} theSelf
 * @param {Object} d
 * @param {Object} nodeData
 */
var dataSetUpdate = function(theSelf, d, nodeData) {
	var theSelfObj = d3.select(theSelf);
	theSelfObj.attr("transform", "translate(" + (nodeData.x - nodeData.width / 2) + "," + (nodeData.y - nodeData.height / 2) + ")scale(1,1)");
}

/**
 * 
 * @param {Object} keys
 * @param {Object} obj
 */
var layOutDataSet = function(keys, obj) {
	if(keys || keys.length > 0) {
		g_dataSet.setNode(keys, obj);
		dataSetList.push({
			k: keys,
			v: {
				name: obj.label,
				location: '',
				dataFormat: '',
				recordType: '',
				instance: ''
			}
		});
		var edges = g_dataSet.edges();
		$.each(edges, function(i, d) {
			g_dataSet.removeEdge(d.v, d.w);
		});
		each(dataSetList, function(i, o) {
			if(i < dataSetList.length - 1) {
				g_dataSet.setEdge(dataSetList[i].k, dataSetList[(i + 1)].k);
				return true;
			}
			return false;
		});
		_dataset._build();
	}
}

/**
 * 
 */
var dataSetList = [];

/**
 * 
 */
var dataSet_Click = function(headerTxt, gId) {
		$("#hidDataSetId").val(gId);
		var e = window.event || arguments.callee.caller.arguments[0];
		var o = getEventSources(e);
		document.getElementById("divleftdatasetproperty").style.display = "block";
		document.getElementById("datasetcssheader").innerHTML = document.getElementById(headerTxt).innerHTML;
		each(dataSetList, function() {
			if(this.k.toString().localeCompare(gId) == 0) {
				console.log(this.v);
				$("#dataset_property_name").val(this.v.name);
				$("#dataset_property_location").val(this.v.location);
				$("#dataset_property_dataFormat").val(this.v.dataFormat);
				$("#dataset_property_recordType").val(this.v.recordType);
				$("#dataset_property_instance").val(this.v.instance);
				return false;
			}
			return true;
		});
	}
	/**
	 * 改变dataSet属性变化
	 */
var changeDataSetProeroty = function() {
	var e = window.event || arguments.callee.caller.arguments[0];
	var o = getEventSources(e);
	var tempId = o.getAttribute("id");
	var tempProperty = tempId.substring(tempId.lastIndexOf("_") + 1);
	console.log("------------------------------------------");
	var gId = $("#hidDataSetId").val();
	each(dataSetList, function() {
		if(this.k.toString().localeCompare(gId) == 0) {
			this.v[tempProperty] = o.value;
			if(tempProperty.localeCompare("name")==0){
				$("#datasetcssheader").html(o.value);
				$("#dataSetText"+gId).html(o.value);
			}
			return false;
		} else {
			return true;
		}
	});
}