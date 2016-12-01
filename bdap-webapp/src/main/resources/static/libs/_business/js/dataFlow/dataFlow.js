var _node = {
	start: function() {
		var temp = new Date().getTime();
		nodeLists.push("g_" + temp);
		app.init("g_" + temp).setA({
			"type": "circle",
			"args": "30"
		}).append("circle", "circle_" + temp, "g_" + temp).setA({
			"r": "30",
			"class": "startNode"
		}).add("text", "text_" + temp, "g_" + temp).setA({
			"x": "-12",
			"y": "5"
		}).txt("Star");
	},
	end: function() {
		var temp = new Date().getTime();
		nodeLists.push("g_" + temp);
		app.init("g_" + temp).setA({
			"type": "circle",
			"args": "30"
		}).append("circle", "circle_" + temp, "g_" + temp).setA({
			"r": "30",
			"class": "endNode"
		}).add("text", "text_" + temp, "g_" + temp).setA({
			"x": "-12",
			"y": "5"
		}).txt("End");
	},
	ActionOne: function() {
		
		var temp = new Date().getTime();
		nodeLists.push("g_" + temp);
		app.init("g_" + temp).setA({
				"type": "rect",
				"args": "100,40"
			}).add("g", "g_" + temp + "_min", "g_" + temp).setA({
				"type": "rect",
				"args": "100,40",
				"class": "rectNodeG"
			}).append("rect", "rect_" + temp, "g_" + temp).setA({
				"rx": "5",
				"ry": "5",
				"width": "100",
				"height": "40"
			}).add("text", "gtext_" + temp, "g_" + temp + "_min", "g_" + temp).setA({
				"x": "30",
				"y": "25"
			}).txt("Action")
			.add("circle", "circle_" + temp, "g_" + temp + "_min", "g_" + temp).setA({
				"r": "6",
				"cx": "90",
				"cy": "20",
				"self": "dialog",
				"args": ""
			}).add("path", "path_" + temp + "_min", "g_" + temp + "_min", "g_" + temp).setA({
				"d": "M86,20 L94,20 M90,16 L90,24",
				"self": "dialog",
				"args": "open"
			}).add("path", "pathRun_" + temp, "g_" + temp).setA({
				"d": "M10,12 L28,19 L10,28 Z",
				"self": "run",
				"args": "play",
				"class": "rectNodePlay"
			}).add("g", "g_" + temp + "_max", "g_" + temp).setA({
				"type": "rect",
				"args": "200,80",
				"transform": "translate(0,0)scale(0,0)",
				"class": "rectNodeGMax"
			}).append("rect", "rect_" + temp + "_max", "g_" + temp).setA({
				"width": "400",
				"height": "200",
				"rx": "5",
				"ry": "5"
			}).add("circle", "circle_" + temp + "_max", "g_" + temp + "_max", "g_" + temp).setA({
				"r": "10",
				"cx": "385",
				"cy": "15",
				"self": "dialog",
				"args": ""
			}).add("path", "path_" + temp + "_max", "g_" + temp + "_max", "g_" + temp).setA({
				"self": "dialog",
				"args": "close",
				"d": "M378,15 L392,15"
			}).add("text", "text_" + temp + "_max", "g_" + temp + "_max", "g_" + temp).setA({
				"x": "185",
				"y": "25"
			}).setS({
				"font-size": "20px"
			})
			.txt("Action")
			.add("text", "text_" + temp + "_max_1", "g_" + temp + "_max", "g_" + temp)
			.setA({
				"x": "10",
				"y": "45"
			})
			.txt("Property_1:XXXXXXXXXXXXXXXXX")
			.add("text", "text_" + temp + "_max_2", "g_" + temp + "_max", "g_" + temp)
			.setA({
				"x": "10",
				"y": "65"
			})
			.txt("Property_2:XXXXXXXXXXXXXXXXX");
	},
	ActionFlow: function() {
		var temp = new Date().getTime();
		nodeLists.push("g_" + temp);
		app.init("g_" + temp).setA({
				"type": "rect",
				"args": "100,80"
			}).append("g", "g_" + temp + "_min").setA({
				"class": "rectNodeG"
			})
			.append("rect", "rect_" + temp + "_min", "g_" + temp).setA({
				"width": "100",
				"height": "80",
				"rx": "5",
				"ry": "5"
			}).add("circle", "circle_" + temp + "_min", "g_" + temp + "_min", "g_" + temp).setA({
				"r": "6",
				"cx": "90",
				"cy": "10",
				"self": "dialog",
				"args": ""				
			}).add("path","path_"+temp+"_min","g_" + temp + "_min", "g_" + temp).setA({
				"d":"M86,10 L94,10 M90,6 L90,14",
				"self": "dialog",
				"args": "open"				
			}).add("text","text_"+temp+"_min","g_" + temp + "_min", "g_" + temp).setA({
				"x": "15",
				"y": "45"
			}).setS({
				"font-size": "15px"
			}).txt("ActionFlow")
	}
}

var _circle = {
	_o: null,
	init: function(o) {
		this._o = o;
		var _self = "";
		var _args = "";
		if(o.getAttribute("self")) {
			_self = o.getAttribute("self").toString();
		}
		if(o.getAttribute("args")) {
			_args = o.getAttribute("args").toString();
		}
		switch(_self) {
			case "dialog":
				this.dialog();
				break;
		}
	},
	dialog: function() {
		_path.init(this._o.nextSibling);
	}
}

var _rect = {
	init: function(o) {

	}
}

var _svg = {
	clearSelectedRect: function(o) {
		var temp = o.getAttribute("G") || "";
		each(nodeLists, function(txt) {
			if(this.toString().localeCompare(temp) == 0) {
				d3.select("#" + this + "_min").attr("class", "rectNodeGSelected");
			} else {
				d3.select("#" + this + "_min").attr("class", "rectNodeG");
			}
		});
	}
}

var _path = {
	_o: null,
	init: function(o) {
		this._o = o;
		var _self = "";
		var _args = "";
		if(o.getAttribute("self")) {
			_self = o.getAttribute("self").toString();
		}
		if(o.getAttribute("args")) {
			_args = o.getAttribute("args").toString();
		}
		switch(_self) {
			case "dialog":
				this.dialog(_args);
				break;
			case "run":
				this.run(_args);
				break;
		}
	},
	dialog: function(args) {
		var g_temp = this._o.getAttribute("G");
		g_temp = g_temp + "_max";
		var temp_transform = "";
		temp_transform = d3.select("#" + g_temp).attr("transform");
		if(args.localeCompare("open") == 0) {
			//放大
			temp_transform = temp_transform.replace("scale(0,0)", "scale(1,1)");
			app.find(g_temp).setTranslate(500, "transform", temp_transform);

		} else if(args.localeCompare("close") == 0) {
			//缩小
			temp_transform = temp_transform.replace("scale(1,1)", "scale(0,0)");
			app.find(g_temp).setTranslate(500, "transform", temp_transform);
		}
	},
	run: function(args) {
		if(args.localeCompare("play") == 0) {
			app.find(this._o.id).setA({
				"args": "stop",
				"d": "M10,12 L25,12 L25,28 L10,28 Z",
				"class": "rectNodeStop"
			});
		} else if(args.localeCompare("stop") == 0) {
			app.find(this._o.id).setA({
				"args": "play",
				"d": "M10,12 L28,19 L10,28 Z",
				"class": "rectNodePlay"
			});
		}

	}
}