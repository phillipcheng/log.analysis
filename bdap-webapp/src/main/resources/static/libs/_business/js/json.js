var _start_node = function(txtId, labelTxt) {
	var obj = {
		id: txtId,
		class: 'nodeG start',
		width: _start_node_min_width,
		height: _start_node_min_height,
		state: 'start',
		transform: 'translate(0,0)scale(1,1)',
		rect: {
			id: 'rect_' + txtId,
			self: 'RECT',
			rx: 5,
			ry: 5,
			onmousedown: '_event.line_onmousedown()',
			onmouseup: '_event.line_onmouseup()'
		},
		run: {
			id: 'run_' + txtId,
			self: 'RUN',
			d: "M0,0L0,10L10,5Z ",
			class: 'nodeRun'
		},
		txt: {
			id: 'txt_' + txtId,
			txt: labelTxt,
			x: 20,
			y: 30,
			onmousedown: '_event.line_onmousedown()',
			onmouseup: '_event.line_onmouseup()'
		},
		pro: {
			id: 'pro_g_' + txtId,
			class: 'minNodeG',
			self: 'ShowProperty',
			transform: 'translate(70,10)scale(1,1)',
			onclick: '_event.property_click()',
			circle: {
				id: 'pro_circle_' + txtId,
				class: 'minNodeCircle',
				self: 'ShowProperty',
				r: '5'
			},
			path: {
				id: 'pro_path_' + txtId,
				class: 'minNodePath',
				self: 'ShowProperty',
				d: 'M-3,0L3,0M0,-3L0,3'
			}
		}

	};
	return obj;
}

var _end_node = function(txtId, labelTxt) {
	var obj = {
		id: txtId,
		class: 'nodeG end',
		width: _end_node_min_width,
		height: _end_node_min_height,
		state: 'end',
		transform: 'translate(0,0)scale(1,1)',
		rect: {
			id: 'rect_' + txtId,
			self: 'RECT',
			rx: 5,
			ry: 5,
			onmousedown: '_event.line_onmousedown()',
			onmouseup: '_event.line_onmouseup()'
		},
		run: {
			id: 'run_' + txtId,
			self: 'RUN',
			d: "M0,0L0,10L10,5Z ",
			class: 'nodeRun'
		},
		txt: {
			id: 'txt_' + txtId,
			txt: labelTxt,
			x: 20,
			y: 30,
			onmousedown: '_event.line_onmousedown()',
			onmouseup: '_event.line_onmouseup()'
		},
		pro: {
			id: 'pro_g_' + txtId,
			class: 'minNodeG',
			self: 'ShowProperty',
			transform: 'translate(70,10)scale(1,1)',
			onclick: '_event.property_click()',
			circle: {
				id: 'pro_circle_' + txtId,
				class: 'minNodeCircle',
				self: 'ShowProperty',
				r: '5'
			},
			path: {
				id: 'pro_path_' + txtId,
				class: 'minNodePath',
				self: 'ShowProperty',
				d: 'M-3,0L3,0M0,-3L0,3'
			}
		}

	};
	return obj;
}

var _action_node = function(txtId, labelTxt) {
	var obj = {
		id: txtId,
		G: txtId,
		class: 'nodeG action',
		width: _action_node_min_width,
		height: _action_node_min_height,
		state: 'action',
		transform: 'translate(0,0)scale(1,1)',
		rect: {
			id: 'rect_' + txtId,
			self: 'RECT',
			rx: 5,
			ry: 5,
			onmousedown: '_event.line_onmousedown()',
			onmouseup: '_event.line_onmouseup()'
		},

		run: {
			id: 'run_' + txtId,
			self: 'RUN',
			d: "M0,0L0,10L10,5Z ",
			class: 'nodeRun'
		},

		txt: {
			id: 'txt_' + txtId,
			txt: labelTxt,
			x: 20,
			y: 25,
			onmousedown: '_event.line_onmousedown()',
			onmouseup: '_event.line_onmouseup()'
		},

		pro: {
			id: 'pro_g_' + txtId,
			class: 'minNodeG',
			self: 'ShowProperty',
			transform: 'translate(190,10)scale(1,1)',
			onclick: '_event.property_click()',
			circle: {
				id: 'pro_circle_' + txtId,
				class: 'minNodeCircle',
				self: 'ShowProperty',
				r: '5'
			},
			path: {
				id: 'pro_path_' + txtId,
				class: 'minNodePath',
				self: 'ShowProperty',
				d: 'M-3,0L3,0M0,-3L0,3'
			}
		},
		addIn: {
			id: 'addIn_g_' + txtId,
			class: 'minNodeG inletsMinNodeG',
			self: 'addInLetsPoint',
			transform: 'translate(10,15)scale(1,1)',
			onclick: '_event.data_addIn_click()',
			circle: {
				id: 'addIn_circle_' + txtId,
				class: 'letsMinNodeCircle',
				self: 'addInLetsPoint',
				r: '5'
			},
			path: {
				id: 'addIn_path_' + txtId,
				class: 'letsMinNodePath',
				self: 'addInLetsPoint',
				d: 'M-3,0L3,0M0,-3L0,3'
			}
		},
		addOut: {
			id: 'addOut_g_' + txtId,
			class: 'minNodeG outletsMinNodeG',
			self: 'addOutLetsPoint',
			transform: 'translate(10,35)scale(1,1)',
			onclick: '_event.data_addOut_click()',
			circle: {
				id: 'addOut_circle_' + txtId,
				class: 'letsMinNodeCircle',
				self: 'addOutLetsPoint',
				r: '5'
			},
			path: {
				d: 'addOut_path_' + txtId,
				class: 'letsMinNodePath',
				self: 'addOutLetsPoint',
				d: 'M-3,0L3,0M0,-3L0,3'
			}
		}
	};
	return obj;
}

var _In_data_node = function(txtId , gId) {
	var obj = {
		id: txtId,
		G: gId,
		class: 'nodeChildG',
		width: 0,
		height: 0,
		onclick:'_event.selectedData(\"'+txtId+'\",\"'+gId+'\")',
		rect: {
			id: txtId + "_rect",
			G: gId,
			width: 0,
			height: 0
		}
	}
	return obj;
}

var _Out_data_node = function(txtId , gId) {
	var obj = {
		id: txtId,
		class: 'nodeChildG',
		width: 0,
		height: 0,
		G:gId,
		show:false,
		onclick:'_event.selectedData(\"'+txtId+'\",\"'+gId+'\")',
		rect:{
			id: txtId + "_rect",
			G: gId,
			width: 0,
			height: 0
		}
	}
	return obj;
}

var _Property_node = function(txtId, gId) {
	var obj = {
		id: txtId,
		class: 'nodeChildG',
		width: 0,
		height: 0,
		line: false,
		G: gId,
		rect: {
			id: txtId + "_rect",
			G: gId,
			width: 0,
			height: 0,
			onclick:'_event.selectedProperty()'
		}
	}
	return obj;
}

var _group_node = function(txtId) {
	var obj = {
		id: txtId,
		width: 121,
		height: 100,
		state: 'group',
		display: false,
		class: 'nodeG action'
	}
	return obj;
}

var aryDB = [];

var db = {
	_findObject: function(keys) {
		var result = {};
		each(aryDB, function() {
			if(this.k.localeCompare(keys) == 0) {
				result = this.v;
				return false;
			}
			return true
		});
		return result;
	},
	_saveObject: function(keys, obj) {
		var booleanIsExist = false;
		each(aryDB, function() {
			if(this.k.localeCompare(keys) == 0) {
				booleanIsExist = true;
				this.v = obj;
				return false;
			}
			return true;
		});
		if(!booleanIsExist) {
			aryDB.push({
				k: keys,
				v: obj
			});
		}
	},
	_deleteObject: function(keys) {
		each(aryDB, function(i, o) {
			if(this.k.localeCompare(keys) == 0) {
				aryDB.splice(i, 1);
				return false;
			}
			return true;
		});
	}
}