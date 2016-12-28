var app = {
	/**
	 * mainkey is main key. 
	 * if key is null, it will generate a new key.
	 * when load action, it will put the parameter.
	 * mainkey is only used by load flow.
	 * 
	 */
	start: function(mainkey) {
		nodeIndex++;
		var temp_g = "g_" + (new Date().getTime() + nodeIndex);
		if(!isEmpty(mainkey)){
			temp_g = mainkey;
		}
		nodeArgs.push({
			k: temp_g,
			vmax: {
				width: '300',
				height: '50'
			},
			vmin: {
				width: '80',
				height: '50'
			}
		});
		var obj = {
			label: "start",
			width: 80,
			height: 50,
			nodeType: 'start',
			runstate: 'play',
			action: 'node',
			zoom: 'normal'
		};
		setNodeSelf(temp_g, obj);
		var propertyObj = {
			'@class': 'start',
			'name': 'start',
			'outlets': [],
			'duration': ''
		}
		setPropertySelf(temp_g, propertyObj, 4);
		_base._build();
	},
	/**
	 * mainkey is main key. 
	 * if key is null, it will generate a new key.
	 * when load action, it will put the parameter.
	 * mainkey is only used by load flow.
	 * 
	 */
	end: function(mainkey) {
		nodeIndex++;
		var temp_g = "g_" + (new Date().getTime() + nodeIndex);
		if(!isEmpty(mainkey)){
			temp_g = mainkey;
		}
		nodeArgs.push({
			k: temp_g,
			vmax: {
				width: '300',
				height: '50'
			},
			vmin: {
				width: '80',
				height: '50'
			}
		});
		var obj = {
			label: "end",
			width: 80,
			height: 50,
			nodeType: 'end',
			runstate: 'play',
			action: 'node',
			zoom: 'normal'
		};
		setNodeSelf(temp_g, obj);
		var propertyObj = {
			'@class': 'end',
			'name': 'end',
			'inLets': []
		}
		setPropertySelf(temp_g, propertyObj, 4);
		_base._build();
	},
	/**
	 * mainkey is main key. 
	 * if key is null, it will generate a new key.
	 * when load action, it will put the parameter.
	 * mainkey is only used by load flow.
	 * 
	 */
	action: function(jsonObj, mainkey) {
		nodeIndex++;
		var temp_g = "g_" + (new Date().getTime() + nodeIndex);
		if(!isEmpty(mainkey)){
			temp_g = mainkey;
		}
		nodeArgs.push({
			k: temp_g,
			vmax: {
				width: '300',
				height: '50'
			},
			vmin: {
				width: '200',
				height: '50'
			}
		});
		var obj = {
			label: jsonObj.label,
			width: 200,
			height: 50,
			nodeType:'action',
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
				propertyObj['@class'] = 'action';
				propertyObj['cmd.class'] = '' + jsonObj.class + '';
				propertyObj['name'] = '' + jsonObj.label + '';
				propertyObj['inLets'] = [];
				propertyObj['outlets'] = [];
				propertyObj['input.format'] = '';
				setPropertySelf(temp_g, propertyObj,_index);
				return false;
			}
		});
	},
	dataSet: function() {
		nodeIndex++;
		var temp_g = "dataset_" + (new Date().getTime() + nodeIndex);
		var obj = {
			label: 'data'+nodeIndex,
			width: 200,
			height: 30,
			dataType: 'IN'
		};
		layOutDataSet(temp_g,obj);
	},
	dataSetShow:function(){
		document.getElementById("dataset_svg").style.display = "block";
	},
	dataSetHide:function(){
		document.getElementById("dataset_svg").style.display = "none";
		document.getElementById("divleftdatasetproperty").style.display = "none";
	}
}