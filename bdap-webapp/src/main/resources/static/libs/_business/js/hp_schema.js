var schemaProperty = null;

function Schema(obj) {
	this.obj = obj;
}

/**
 * 节点拷贝
 * @param {Object} chileNode
 * @param {Object} parentNode
 */
function copyExtends(chileNode,parentNode){
	for(var k in parentNode){
		if(chileNode[k]){
			
		}else{
			chileNode[k] = parentNode[k];
		}
	}
	return chileNode;
};

/**
 * 
 * @param {Object} query  123/123/45
 * 123/123/45
 */
Schema.prototype.findPath = function(query) {
	var ary = query.split("/");
	var o = this.obj || {};
	for(var i = 0; i < ary.length; i++) {
		o = o[ary[i]];
		if(o) {
			continue;
		} else {
			break;
		}
	}
	return o;
}

