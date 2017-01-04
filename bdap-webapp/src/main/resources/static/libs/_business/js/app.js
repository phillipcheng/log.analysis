var nodeIndex = 0;
var result = {
	'@class': "flow",
	name: "flow1",
	outlets: [],
	nodes: [],
	links: [],
	data: [],
	inLets: [],
	wfName: "flow1"
};
var app = {
	start: function(mainkey) {
		nodeIndex++;
		var gId = "";
		if(!isEmpty(mainkey)) {
			gId = mainkey;
		} else {
			gId = "g_" + (new Date().getTime() + nodeIndex);
		}
		result.nodes.push({
			'id': gId,
			'@class': 'start',
			'name': 'start',
			'outlets': [{
				id: gId + '_OutData_0',
				name: '',
				dataName: '',
				show: false,
				state: 'show'
			}, {
				id: gId + '_OutData_1',
				name: '',
				dataName: '',
				show: false,
				state: 'show'
			}, {
				id: gId + '_OutData_2',
				name: '',
				dataName: '',
				show: false,
				state: 'show'
			}, {
				id: gId + '_OutData_3',
				name: '',
				dataName: '',
				show: false,
				state: 'show'
			}],
			'duration': '300'
		});
		var obj = _start_node(gId, 'start');
		g.setNode(gId, obj);
		_build._build();

		var g_child_Instance = childsvg.find(gId);
		if(g_child_Instance == null) {
			g_child_Instance = childsvg.initNewInstance(gId);
		}

		var In_data_instance_0 = _In_data_node(gId + "_InData_0", gId);
		g_child_Instance.setNode(gId + "_InData_0", In_data_instance_0);

		var In_data_instance_1 = _In_data_node(gId + "_InData_1", gId);
		g_child_Instance.setNode(gId + "_InData_1", In_data_instance_1);

		var In_data_instance_2 = _In_data_node(gId + "_InData_2", gId);
		g_child_Instance.setNode(gId + "_InData_2", In_data_instance_2);

		var In_data_instance_3 = _In_data_node(gId + "_InData_3", gId);
		g_child_Instance.setNode(gId + "_InData_3", In_data_instance_3);

		g_child_Instance.setEdge(gId + "_InData_0", gId + "_InData_1");
		g_child_Instance.setEdge(gId + "_InData_1", gId + "_InData_2");
		g_child_Instance.setEdge(gId + "_InData_2", gId + "_InData_3");

		var property_node_instance = _Property_node(gId + "_property", gId);
		g_child_Instance.setNode(gId + "_property", property_node_instance);

		var Out_data_instance_0 = _Out_data_node(gId + "_OutData_0", gId);
		g_child_Instance.setNode(gId + "_OutData_0", Out_data_instance_0);

		var Out_data_instance_1 = _Out_data_node(gId + "_OutData_1", gId);
		g_child_Instance.setNode(gId + "_OutData_1", Out_data_instance_1);

		var Out_data_instance_2 = _Out_data_node(gId + "_OutData_2", gId);
		g_child_Instance.setNode(gId + "_OutData_2", Out_data_instance_2);

		var Out_data_instance_3 = _Out_data_node(gId + "_OutData_3", gId);
		g_child_Instance.setNode(gId + "_OutData_3", Out_data_instance_3);

		g_child_Instance.setEdge(gId + "_OutData_0", gId + "_OutData_1");
		g_child_Instance.setEdge(gId + "_OutData_1", gId + "_OutData_2");
		g_child_Instance.setEdge(gId + "_OutData_2", gId + "_OutData_3");

		_build._build(gId, g_child_Instance);

	},
	end: function(mainkey) {
		nodeIndex++;
		var gId = "";
		if(!isEmpty(mainkey)) {
			gId = mainkey;
		} else {
			gId = "g_" + (new Date().getTime() + nodeIndex);
		}
		result.nodes.push({
			'id': gId,
			'@class': 'end',
			'name': 'end',
			'inLets': [{
				id: gId + '_InData_0',
				name: '',
				dataName: '',
				show: false,
				state: 'show'
			}, {
				id: gId + '_InData_1',
				name: '',
				dataName: '',
				show: false,
				state: 'show'
			}, {
				id: gId + '_InData_2',
				name: '',
				dataName: '',
				show: false,
				state: 'show'
			}, {
				id: gId + '_InData_3',
				name: '',
				dataName: '',
				show: false,
				state: 'show'
			}]
		});
		var obj = _end_node(gId, 'end');
		g.setNode(gId, obj);
		_build._build();

		var g_child_Instance = childsvg.find(gId);
		if(g_child_Instance == null) {
			g_child_Instance = childsvg.initNewInstance(gId);
		}

		var In_data_instance_0 = _In_data_node(gId + "_InData_0", gId);
		g_child_Instance.setNode(gId + "_InData_0", In_data_instance_0);

		var In_data_instance_1 = _In_data_node(gId + "_InData_1", gId);
		g_child_Instance.setNode(gId + "_InData_1", In_data_instance_1);

		var In_data_instance_2 = _In_data_node(gId + "_InData_2", gId);
		g_child_Instance.setNode(gId + "_InData_2", In_data_instance_2);

		var In_data_instance_3 = _In_data_node(gId + "_InData_3", gId);
		g_child_Instance.setNode(gId + "_InData_3", In_data_instance_3);

		g_child_Instance.setEdge(gId + "_InData_0", gId + "_InData_1");
		g_child_Instance.setEdge(gId + "_InData_1", gId + "_InData_2");
		g_child_Instance.setEdge(gId + "_InData_2", gId + "_InData_3");

		var property_node_instance = _Property_node(gId + "_property", gId);
		g_child_Instance.setNode(gId + "_property", property_node_instance);

		var Out_data_instance_0 = _Out_data_node(gId + "_OutData_0", gId);
		g_child_Instance.setNode(gId + "_OutData_0", Out_data_instance_0);

		var Out_data_instance_1 = _Out_data_node(gId + "_OutData_1", gId);
		g_child_Instance.setNode(gId + "_OutData_1", Out_data_instance_1);

		var Out_data_instance_2 = _Out_data_node(gId + "_OutData_2", gId);
		g_child_Instance.setNode(gId + "_OutData_2", Out_data_instance_2);

		var Out_data_instance_3 = _Out_data_node(gId + "_OutData_3", gId);
		g_child_Instance.setNode(gId + "_OutData_3", Out_data_instance_3);

		g_child_Instance.setEdge(gId + "_OutData_0", gId + "_OutData_1");
		g_child_Instance.setEdge(gId + "_OutData_1", gId + "_OutData_2");
		g_child_Instance.setEdge(gId + "_OutData_2", gId + "_OutData_3");

		_build._build(gId, g_child_Instance);

	},
	action: function(jsonObj, mainkey) {
		nodeIndex++;
		var gId = "";
		var tempcla = jsonObj.cla;
		if(!isEmpty(mainkey)) {
			gId = mainkey;
		} else {
			gId = "g_" + (new Date().getTime() + nodeIndex);
		}
		$.each(remoteActionObj, function(k, v) {
			if(k.localeCompare(jsonObj.cla) == 0) {
				var propertyObj = {};
				propertyObj["id"] = gId.toString();
				propertyObj['@class'] = 'action';
				propertyObj['cmd.class'] = '' + jsonObj.cla + '';
				propertyObj['name'] = '' + jsonObj.label + '';
				propertyObj['inLets'] = [{
					id: gId + '_InData_0',
					name: '',
					dataName: '',
					show: false,
					state: 'show'
				}, {
					id: gId + '_InData_1',
					name: '',
					dataName: '',
					show: false,
					state: 'show'
				}, {
					id: gId + '_InData_2',
					name: '',
					dataName: '',
					show: false,
					state: 'show'
				}, {
					id: gId + '_InData_3',
					name: '',
					dataName: '',
					show: false,
					state: 'show'
				}];
				propertyObj['outlets'] = [{
					id: gId + '_OutData_0',
					name: '',
					dataName: '',
					show: false,
					state: 'show'
				}, {
					id: gId + '_OutData_1',
					name: '',
					dataName: '',
					show: false,
					state: 'show'
				}, {
					id: gId + '_OutData_2',
					name: '',
					dataName: '',
					show: false,
					state: 'show'
				}, {
					id: gId + '_OutData_3',
					name: '',
					dataName: '',
					show: false,
					state: 'show'
				}];
				propertyObj['input.format'] = '';
				each(v, function(i, o) {
					propertyObj[o.toString()] = '';
					return true;
				});
				result.nodes.push(propertyObj);
				var obj = _action_node(gId, jsonObj.label);
				g.setNode(gId, obj);
				_build._build();

				var g_child_Instance = childsvg.find(gId);
				if(g_child_Instance == null) {
					g_child_Instance = childsvg.initNewInstance(gId);
				}

				var In_data_instance_0 = _In_data_node(gId + "_InData_0", gId);
				g_child_Instance.setNode(gId + "_InData_0", In_data_instance_0);

				var In_data_instance_1 = _In_data_node(gId + "_InData_1", gId);
				g_child_Instance.setNode(gId + "_InData_1", In_data_instance_1);

				var In_data_instance_2 = _In_data_node(gId + "_InData_2", gId);
				g_child_Instance.setNode(gId + "_InData_2", In_data_instance_2);

				var In_data_instance_3 = _In_data_node(gId + "_InData_3", gId);
				g_child_Instance.setNode(gId + "_InData_3", In_data_instance_3);

				g_child_Instance.setEdge(gId + "_InData_0", gId + "_InData_1");
				g_child_Instance.setEdge(gId + "_InData_1", gId + "_InData_2");
				g_child_Instance.setEdge(gId + "_InData_2", gId + "_InData_3");

				var property_node_instance = _Property_node(gId + "_property", gId);
				g_child_Instance.setNode(gId + "_property", property_node_instance);

				var Out_data_instance_0 = _Out_data_node(gId + "_OutData_0", gId);
				g_child_Instance.setNode(gId + "_OutData_0", Out_data_instance_0);

				var Out_data_instance_1 = _Out_data_node(gId + "_OutData_1", gId);
				g_child_Instance.setNode(gId + "_OutData_1", Out_data_instance_1);

				var Out_data_instance_2 = _Out_data_node(gId + "_OutData_2", gId);
				g_child_Instance.setNode(gId + "_OutData_2", Out_data_instance_2);

				var Out_data_instance_3 = _Out_data_node(gId + "_OutData_3", gId);
				g_child_Instance.setNode(gId + "_OutData_3", Out_data_instance_3);

				g_child_Instance.setEdge(gId + "_OutData_0", gId + "_OutData_1");
				g_child_Instance.setEdge(gId + "_OutData_1", gId + "_OutData_2");
				g_child_Instance.setEdge(gId + "_OutData_2", gId + "_OutData_3");

				_build._build(gId, g_child_Instance);

				return false;
			}
		});
	},
	dataSet: function() {
		nodeIndex++;
		var temp_g = "dataset_" + (new Date().getTime() + nodeIndex);
		addLeftDiv(temp_g);
	},
	save: function() {
		var thisObj = this;
		var saveResult={};
		//deep copy
		saveResult = $.extend(true,{}, result);;
		function findDataNewId(findId) {
			var txt = "";
			each(saveResult.data, function() {
				if(this.id.localeCompare(findId) == 0) {
					txt = this.name;
					return false;
				}
				return true;
			});
			return txt;
		}

		//
		each(saveResult.data, function() {
			this.name = this.name + "_" + this.id;
			return true;
		});
		
		each(saveResult.nodes, function() {
			this.name = this.id;
			return true;
		});		

		each(saveResult.nodes, function() {
			var tempInLets = [];
			var tempOutLets = [];
			if(this.inLets) {
				each(this.inLets, function(i, o) {
					if(o.show) {
						var obj = {};
						obj.name = o.name;
						obj.dataName = findDataNewId(o.dataName);
						tempInLets.push(obj);
					}
					return true;
				});
				this.inLets = tempInLets;
			}

			if(this.outlets) {
				each(this.outlets, function(i, o) {
					if(o.show) {
						var obj = {};
						obj.name = o.name;
						obj.dataName = findDataNewId(o.dataName);
						tempOutLets.push(obj);
					}
					return true;
				});
				this.outlets = tempOutLets;
			}

			return true;
		});
		
//		each(result.links,function(){
//			
//			return true;
//		});
		
		console.log("result", JSON.stringify(saveResult) );
		thisObj.saveAsJson(saveResult);
	},
	saveAsJson : function(saveResult) {
		$.ajax({
			type: "post",
			url: getAjaxAbsolutePath(_HTTP_SAVE_JSON),
			contentType: 'application/json',
			data: JSON.stringify(saveResult),
			//dataType: "json",
			success: function(data, textStatus) {
				console.info(data);
			},
			error: function(e) {
				console.info(e);
			}
		});

	}
}