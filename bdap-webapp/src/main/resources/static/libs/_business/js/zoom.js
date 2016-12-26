/**
 * 
 */
var zoom = {
	ShowProperty: function(keys) {
		var nodeData = g.node(keys);
		var temp_width = 0;
		each(nodeArgs,function(){
			if(this.k.toString().localeCompare(keys)==0){
				temp_width = this.vmax.width;
				return false;
			}else{
				return true;
			}
		});
		each(propertyList, function(i, o) {
			if(o.k.toString().localeCompare(keys) == 0) {
				var temp_index = 0;
				$.each(o.v, function() {
					temp_index++;
				});
				var obj = {
					label: nodeData.label || "",
					width: temp_width,
					height: ((18 * (temp_index)) + 10),
					nodeType: nodeData.nodeType,
					runstate: nodeData.runstate || 'play',
					action: nodeData.action || 'node',
					zoom: 'max'
				}
				setNodeSelf(keys, obj);
				_base._build();
				return false;
			} else {
				return true;
			}
		});
	},
	HideProperty: function(keys) {
		var nodeData = g.node(keys);
		var temp_width = 0;
		var temp_height = 0;
		each(nodeArgs,function(){
			if(this.k.toString().localeCompare(keys)==0){
				temp_width = this.vmin.width;
				temp_height = this.vmin.height;
				return false;
			}else{
				return true;
			}
		});		
		var obj = {
			label: nodeData.label || "",
			width: temp_width,
			height: temp_height,
			nodeType: nodeData.nodeType,
			runstate: nodeData.runstate || 'play',
			action: nodeData.action || 'node',
			zoom: 'normal'
		}
		setNodeSelf(keys, obj)
		_base._build();
	}
}

/**
 * 打开加载属性列表
 * @param {Object} d
 * @param {Object} nodeData
 */
var loadProperty = function(d, nodeData) {
	d3.select("#" + d).select(".nodePropertyG").remove();
	d3.select(".rightupcssbody").selectAll(".sublistgroup").remove();
	d3.select("#divrightup").select(".rightupcssheader").select("strong").text(document.getElementById(d).getElementsByTagName("text")[0].innerHTML);
	each(propertyList, function(i, v) {
		if(v.k.toString().localeCompare(d.toString()) == 0) {
			d3.select("#" + d).append("g")
				.attr("id", "propertylist_" + d)
				.attr("class", "nodePropertyG")
				.append("rect")
				.attr("G", d)
				.attr("width", 280)
				.attr("height", 10)
				.attr("self", "RECT")
				.attr("x", 10)
				.attr("y", 35);
			var _index = 0;
			var svgrect = d3.select("#propertylist_" + d);
			$.each(v.v, function(key, value) {
				if(key.toString().localeCompare("inLets") == 0 || key.toString().localeCompare("outlets") == 0 || key.toString().localeCompare("@class") == 0) {

				} else {
					_index++;
					var theselect = d3.select(".rightupcssbody").append("div").attr("class", "sublistgroup");
					theselect.append("strong").text(key + ":");
					theselect.append("input").attr("type", "text")
						.attr("value", value).attr("placeholder", "...")
						.attr("onkeyup", "changeProperty('" + d + "','" + key + "','propertylist_" + d + "_" + _index + "')");

					svgrect.append("text")
						.attr("id", "propertylist_" + d + "_" + _index)
						.attr("x", 15).attr("G", d)
						.attr("y", 32 + (_index * 17))
						.text(key + ":" + value);
				}
			});
			d3.select("#" + d).select("#propertylist_" + d).select("rect").attr("height", (_index * 18));
			return false
		} else {
			return true;
		}
	});

	d3.select("#divrightup").style({
		"display": "block"
	});

}

/**
 * 属性改变事件
 * @param {Object} d
 * @param {Object} keys
 * @param {Object} proId
 */
var changeProperty = function(d, keys, proId) {
	var e = window.event || arguments.callee.caller.arguments[0];
	d3.select("#" + proId).text(keys + ":" + getEventSources(e).value);
	if(keys.toString().localeCompare("name") == 0) {
		$("#rightupcssheaderstring").html(getEventSources(e).value);
		d3.select("#text_" + d).text(getEventSources(e).value);
		var nodeDate = g.node(d);
		var obj = {
			label: getEventSources(e).value,
			width: nodeDate.width,
			height: nodeDate.height,
			runstate: nodeDate.runstate,
			action: nodeDate.action,
			zoom: nodeDate.zoom
		};
		g.setNode(d, obj);
	}
	each(propertyList, function(i, o) {
		if(o.k.toString().localeCompare(d) == 0) {
			$.each(o.v, function(key, value) {
				if(key.toString().localeCompare(keys.toString()) == 0) {
					o.v[key] = getEventSources(e).value;
					return false;
				}
			});
			return false;
		} else {
			return true;
		}
	});
}

/**
 * 清除一个节点下的所有属性
 * @param {Object} d
 */
var clearProperty = function(d) {
	d3.select("#" + d).select(".nodePropertyG").remove();
}