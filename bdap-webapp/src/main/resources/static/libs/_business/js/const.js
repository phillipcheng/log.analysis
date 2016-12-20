var g_mouse_down = "";

var g_mouse_up = "";

var clientwidth = 0;

var clientheight = 0;

var dataSetDialogId = "";

var countProperty = 0;

var clicktimer = null;

var nodeIndex = 0;

var booleaniszoom = false; //zoom 操作

var selectionId = "";

var current_zoom_x = 0;

var current_zoom_y = 0;

var current_zoom_old_x = 0;

var current_zoom_old_y = 0;

var current_zoom_new_x = 0;

var current_zoom_new_y = 0;

var display_off_left = 0;

var display_off_top = 0;

var remoteActionObj = {};

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
 * 节点列表
 * 
 */
var nodeLists = [];

/**
 *  连接线列表
 */
var pathLists = [];

/**
 * 属性列表
 */
var propertyList = [];

/**
 * 数据属性内容
 */
var dataList = [];

var nodeArgs = [];

// Create a new directed graph
/**
 * 1.1
 * 创建画版
 */
var g = new dagre.graphlib.Graph({
	compound: true
});

var param = {
	name: "test",
	rankdir: 'TB',
	/**
	 * Dagre's nodesep param - number of pixels that
	 * separate nodes horizontally in the layout.
	 *
	 * See https://github.com/cpettitt/dagre/wiki#configuring-the-layout
	 */
	nodeSep: 35,
	/**
	 * Dagre's ranksep param - number of pixels
	 * between each rank in the layout.
	 *
	 * See https://github.com/cpettitt/dagre/wiki#configuring-the-layout
	 */
	rankSep: 25,
	/**
	 * Dagre's edgesep param - number of pixels that separate
	 * edges horizontally in the layout.
	 */
	edgeSep: 5
};

// Set an object for the graph label
g.setGraph(param);

// Default to assigning a new object as a label for each new edge.
g.setDefaultEdgeLabel(function() {
	return {};
});

/**
 * 1.2
 * 贝赛尔曲线的生成器
 * basis
 */
var interpolate = d3.svg.line()
	.interpolate('basis')
	.x(function(d) {
		return d.x;
	})
	.y(function(d) {
		return d.y;
	});

//------------star-------------------------------transition,tweenDash --------------------------------------------------------
/**
 * 2.1
 * @param {Object} path
 */
var transition = function(path) {
	path.transition()
		.duration(2000)
		.attrTween("stroke-dasharray", tweenDash)
		.each("end", function() {
			d3.select(this).call(transition);
		});
}

/**
 * 2.2
 */
var tweenDash = function() {
		var l = this.getTotalLength(),
			i = d3.interpolateString("0," + l, l + "," + l);
		return function(t) {
			return i(t);
		};
	}
	//------------end-------------------------------transition,tweenDash --------------------------------------------------------

var openLogWin = function() {
	var winObj = $('#logDetail').window();
	$('#logDetail').window({
		top: ($(window).height() - winObj.height() - 30),
		left: ($(window).width() - winObj.width() - 18)
	});
	$('#logDetail').window('open');
}

var openDatasetWin = function() {
	var datasetWinObj = $('#datasetDetail').window();
	$('#datasetDetail').window({
		top: 65,
		left: ($(window).width() - datasetWinObj.width() - 18)
	});
	$('#datasetDetail').window('open');

	$('#dg1').datagrid({
		url: 'datagrid_data1.json',
		method: 'get',
		//title: 'dataSet',
		//iconCls: 'icon-save',
		width: '99%',
		height: '97%',
		fitColumns: true,
		singleSelect: true,
		rownumbers: true,
		autoRowHeight: false,
		pagination: true,
		pageSize: 10,
		columns: [
			[{
				field: 'itemid',
				title: 'Item ID',
				width: 80
			}, {
				field: 'productid',
				title: 'Product ID',
				width: 120
			}, {
				field: 'listprice',
				title: 'List Price',
				width: 80,
				align: 'right'
			}, {
				field: 'unitcost',
				title: 'Unit Cost',
				width: 80,
				align: 'right'
			}, {
				field: 'attr1',
				title: 'Attribute',
				width: 250
			}, {
				field: 'status',
				title: 'Status',
				width: 60,
				align: 'center'
			}]
		]
	});
	$('#dg1').datagrid('getPager').pagination({
		onSelectPage: function(pageNo, pageSize) {
			console.info("test2");
			//ajax request back end
			$.ajax({
				type: "get",
				url: "http://www.test.com/rss",
				data: {
					'pageNo': pageNo,
					'pageSize': pageSize
				},
				success: function(data, textStatus) {
					//$("#dg1").datagrid("loadData", data);
				},
				error: function() {
					//请求出错处理
				}
			});

		}
	});
	console.info("test");
}

var loadinit = function() {

	var logWinObj = $('#logDetail').window();
	$('#logDetail').window({
		top: ($(window).height() - logWinObj.height() - 30),
		left: ($(window).width() - logWinObj.width() - 18)
	});
	$('#logDetail').window('close', true);

	var datasetWinObj = $('#datasetDetail').window();
	$('#datasetDetail').window({
		top: 65,
		left: ($(window).width() - datasetWinObj.width() - 18)
	});
	$('#datasetDetail').window('close', true);
	// connectWebSoket();

	/**
	 * 
	 */
	d3.json(getAjaxAbsolutePath(_HTTP_LOAD_ACTION_INFOR), function(data) {
		remoteActionObj = data;
		$.each(data, function(k, v) {
			var temp = k;
			temp = temp.substring(temp.lastIndexOf(".") + 1);
			d3.select("#actionList")
				.append("li").append("a")
				.attr("href", "javascript:;").text(temp)
				//.attr("onclick","app.group()");
				.attr("onclick", "app.action({'label':'" + temp + "','class':'" + k + "'})");
		});
	});
}

/**
 * 
 */
var _HTTP_ACTION_LIST = "http://localhost:8020/flow/actionList";

//var _HTTP_ACTION_LIST="http://localhost:8080/dashview/george/flow/node/types/action/commands";

/**
 * 
 */
var _HTTP_LOAD_PROPERTY = "http://localhost:8020/flow/loadProperty";

/**
 * 
 */
//var _HTTP_LOAD_ACTION_INFOR = "http://localhost:8020/flow/actionInfor";

var _HTTP_LOAD_ACTION_INFOR = "/dashview/george/flow/node/types/action/commands";

/**
 * 保存JSON
 */
var _HTTP_SAVE_JSON = "/dashview/george/flow/";

//var _HTTP_SAVE_JSON = "http://localhost:8080/dashview/george/flow";