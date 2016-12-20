var app = {
	start: function() {
		nodeIndex++;
		var temp_g = "g_" + (new Date().getTime() + nodeIndex);
		var obj = {
			label: "Star",
			width: 100,
			height: 50,
			runstate: 'play',
			action: 'node',
			zoom: 'normal'
		};
		setNodeSelf(temp_g, obj);
		var propertyObj = {
			'@class': 'start',
			'name': 'Star',
			'outlets': [],
			'input.format': ''
		}
		setPropertySelf(temp_g, propertyObj, 4);
		_base._build();
	},
	end: function() {
		nodeIndex++;
		var temp_g = "g_" + (new Date().getTime() + nodeIndex);
		var obj = {
			label: "end",
			width: 100,
			height: 50,
			runstate: 'play',
			action: 'node',
			zoom: 'normal'
		};
		setNodeSelf(temp_g, obj);
		var propertyObj = {
			'@class': 'end',
			'name': 'end',
			'inLets': [],
			'input.format': ''
		}
		setPropertySelf(temp_g, propertyObj, 4);
		_base._build();
	},
	action: function(jsonObj) {
		console.log("jsonObj", jsonObj);
		nodeIndex++;
		var temp_g = "g_" + (new Date().getTime() + nodeIndex);
		var obj = {
			label: jsonObj.label,
			width: 200,
			height: 50,
			runstate: 'play',
			action: 'node',
			zoom: 'normal'
		};
		setNodeSelf(temp_g, obj);
		_base._build();
		$.each(remoteActionObj, function(k, v) {
			if(k.toString().indexOf(jsonObj.label) > -1) {
				var propertyObj = {};
				var _index = 5;
				each(v, function(i, o) {
					_index++;
					propertyObj[o.toString()] = '';
					return true;
				});
				propertyObj['@class'] = '' + jsonObj.class + '';
				propertyObj['name'] = '' + jsonObj.label + '';
				propertyObj['inLets'] = [];
				propertyObj['outlets'] = [];
				propertyObj['input.format'] = '';
				setPropertySelf(temp_g, propertyObj,_index);
				return false;
			}
		});
	}
}