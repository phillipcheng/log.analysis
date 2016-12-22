var dywindow = {
		buildDataWindow : function(winID){
//			var winID = "datasetDetail_001001";
			var div_level_1_html = '<div  class="easyui-window" title="Data Info" style="width:550px;height:300px;padding:0px;"></div>';
			var div_level_1 = $(div_level_1_html);
			div_level_1.attr("id",winID);
			div_level_1.attr("data-options","border:'thin',cls:'c2',minimizable:false,inline:false,constrain:true");
			
			var div_level_2_html = '<div class="easyui-tabs" style="width:100%;height:100%"></div>';
			var div_level_2 = $(div_level_2_html);
			
			//tab dataset
			var div_level_3_1_html = '<div id="tab-dataset" title="dataset" style="padding:0px;margin: 0px;">test1</div>';
			var div_level_3_1 = $(div_level_3_1_html);
			
			//tab dataschame
			var div_level_3_2_html = '<div id="tab-dataschame" title="schame" style="padding:10px">test2</div>';
			var div_level_3_2 = $(div_level_3_2_html);
			
			//tab data 
			var div_level_3_3_html = '<div id="tab-data" title="dataProperty" style="padding:10px"></div>';
			var div_level_3_3 = $(div_level_3_3_html);
			
			var div_level_3_3_context_html_1 = '<div class="sublistgroup"><strong>name:</strong><input id="lets_property_name" type="text" value="" placeholder="..." onkeyup="" /></div>';
			var div_level_3_3_context_html_2 = '<div class="sublistgroup"><strong>dataName:</strong><input id="lets_property_dataName" type="text" value="" placeholder="..." onkeyup="" /></div>';
			div_level_3_3.append($(div_level_3_3_context_html_1)).append($(div_level_3_3_context_html_2));
			
			div_level_2.append(div_level_3_1);
			div_level_2.append(div_level_3_2);
			div_level_2.append(div_level_3_3);
			
			div_level_1.append(div_level_2);
			console.info(div_level_1[0].outerHTML);
			
			$("#windowContentStart").after(div_level_1);
			//the code is very key, it will rerender the window
			$.parser.parse();
			
//			var datasetWinObj = $(winID).window();
			div_level_1.window({
				top: 65,
				left: ($(window).width() - div_level_1.width() - 18)
			});
			div_level_1.window('open');
			
			
			
			
		}
		
		
		
}