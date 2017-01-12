/**
 * 动画延时
 */
var _TRANSITION_DURATION = 500;

var _start_node_min_width = 80;

var _start_node_min_height = 60;


var _end_node_min_width = 80;

var _end_node_min_height = 60;



var _action_node_min_width = 200;

var _action_node_min_height = 60;

var _node_max_width = 300;

var _node_max_property_width = 280;

var WEB_IP = "127.0.0.1";
var WEB_PORT = "8080";
if(window.location.hostname != null && window.location.port != null){
	WEB_IP = window.location.hostname;
	WEB_PORT = window.location.port;
}
console.info(WEB_IP + ":" +WEB_PORT);

/**
 * save JSON
 */
var _HTTP_SAVE_JSON = "/dashview/{userName}/flow/";




var _node_data_width = 60;

var _node_data_height = 40;


/**
 * 用于记录每个节点的属性特征值的内容
 */
var propertyInfor = [];
/**
 * the whole flowname, to edit/view
 */
var WHOLE_FLOW_NAME;
/**
 * the whole instanceid to edit/view
 */
var WHOLE_INSTANCE_ID;
/**
 *  define flow stage, for exmple: design stage / running  / finish to run.  
 */
var FLOW_STAGE = ["DESIGN", "RUNNING", "FINISH", "VIEW"];
/**
 * flow current stage. default design stage.
 */
var FLOW_CURRENT_STAGE = "DESIGN";

/**
 * define runtime flow state.
 */
var FLOW_RUNTIME_STATE =[{"state":"PREP", "color":""},
                         {"state":"RUNNING", "color":""},
                         {"state":"SUCCEEDED", "color":"green"},
                         {"state":"KILLED", "color":"red"},
                         {"state":"FAILED", "color":"red"},
                         {"state":"SUSPENDED", "color":""}];
/**
 * define runtime node state.
 */
var NODE_RUNTIME_STATE = [{"state":"PREP", "color":""},
                          {"state":"RUNNING", "color":""},
                          {"state":"OK", "color":""},
                          {"state":"ERROR", "color":""},
                          {"state":"USER_RETRY", "color":""},
                          {"state":"START_RETRY", "color":""},
                          {"state":"START_MANUAL", "color":""},
                          {"state":"DONE", "color":""},
                          {"state":"END_RETRY", "color":""},
                          {"state":"END_MANUAL", "color":""},
                          {"state":"KILLED", "color":""},
                          {"state":"FAILED", "color":"red"}];

var websocket = null;
var wsURI = "ws://" + WEB_IP +":" + WEB_PORT + "/dashview/ws/george/flow/instances/";


var selfPropertyInfor = {};
