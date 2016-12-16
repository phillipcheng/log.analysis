var zoom = {
	ShowProperty: function(keys) {
		var nodeData = g.node(keys);
		var obj = {
			label: nodeData.label || "",
			width: (nodeData.width || 100) * 1.5,
			height: (nodeData.height || 50) * 8,
			runstate: nodeData.runstate || 'play',
			action: nodeData.action || 'node',
			zoom: 'max'
		}
		setSelfNode(keys, obj);
		_base._build();
	}
}