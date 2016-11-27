/**
 * 节点的类型集合
 */
var actionTypeLists = [];//节点的类型集合

/**
 * 节点的集合
 */
var actionLists = [];//节点的集合

var display = null;

var displayLog = null;

var display_off_left = "";

var display_off_top = "";

/**
 * 选中的对象ID
 */
var selectionId = "";

/**
 * 节点列表
 * 
 */
var nodeLists = [];

/**
 * 所有动态线的集合
 */
var linesLists = [];
/**
 * 用于记录临时点的线的集合
 */
var templine = {
	firstId:'',
	endId:'',
	firstPoint: '',
	middlePoint: '',
	endPoint:'',
	autoMiddle:0
};

/**
 * 表示 是否可以进行 zoom操作
 */
var booleaniszoom = false;


var current_zoom_x = 0;

var current_zoom_y = 0;

var current_zoom_old_x = 0;

var current_zoom_old_y = 0;

var current_zoom_new_x = 0;

var current_zoom_new_y = 0;

