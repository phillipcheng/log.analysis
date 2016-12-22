var dywindow = {
		buildDataWindow : function(winID){
//			var winID = "datasetDetail_001001";
			var div_level_1_html = '<div  class="easyui-window" title="Data Info" style="width:550px;height:300px;padding:0px;"></div>';
			var div_level_1 = $(div_level_1_html);
			div_level_1.attr("id",winID);
			div_level_1.attr("data-options","border:'thin',cls:'c2',minimizable:false,inline:false,constrain:true");
			
			var div_level_2_html = '<div class="easyui-tabs" style="width:100%;height:100%"></div>';
			var div_level_2 = $(div_level_2_html);
			
			var div_level_3_1_html = '<div id="tab-dataset" title="dataset" style="padding:0px;margin: 0px;">test1</div>';
			var div_level_3_1 = $(div_level_3_1_html);
			
			var div_level_3_2_html = '<div id="tab-dataschame" title="schame" style="padding:10px">test2</div>';
			var div_level_3_2 = $(div_level_3_2_html);
			
			var div_level_3_3_html = '<div id="tab-data" title="data" style="padding:10px">test3</div>';
			var div_level_3_3 = $(div_level_3_3_html);
			

			div_level_2.append(div_level_3_1);
			div_level_2.append(div_level_3_2);
			div_level_2.append(div_level_3_3);
			
			div_level_1.append(div_level_2);
			console.info(div_level_1[0].outerHTML);
			
			$("#windowContentStart").after(div_level_1);

			var datasetWinObj = $(winID).window();
			$(winID).window({
				top: 65,
				left: ($(window).width() - datasetWinObj.width() - 18)
			});
			//the code is very key, it will rerender the window
			$.parser.parse();
//			$(winID).window('close', true);
			$(winID).window('open');
			
			
			
			
		}
		
		
		
}