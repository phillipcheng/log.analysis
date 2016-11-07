/**
 * 变量的集合
 * lines = [
 * {id:'',
 * firstId:'',
 * endId:'',
 * firstPoint:'',
 * endPoint:''
 * }
 * ]
 */
var lines = [
	
];

var templine = {
	id: '',
	firstId:'',
	firstPoint: '',
	middlePoint: '',
	endPoint: ''
};

/**
 * 动画主面板
 */
var display = null;

/**
 * 动画SVG
 */
var svg = null;

/**
 * 初始化点的集合
 */
var initTempLine = function() {
	templine.id = "";
	templine.firstId = "";
	templine.firstPoint = "";
	templine.middlePoint = "";
	templine.endPoint = "";
	
	d3.select("#linemove").attr("d", "M0,0 L0,0");
}

/**
 * 移动线
 */
var booleanmoveline = false;

var display_off_left = 0;

var display_off_top = 0;

var booleanoutermove = false;
