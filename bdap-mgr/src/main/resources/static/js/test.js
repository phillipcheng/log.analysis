(function($){
	function getAccoutID(){
		
		var payloadData = '<tsRequest><credentials name="admin" password="admin123" ><site contentUrl="MarketingTeam" /></credentials></tsRequest>';
		
		var aj = $.ajax( {  
			      url:'http://192.238.57.4/api/2.3/auth/signin', 
			      data:payloadData,  
			      headers:{},
			     type:'post',  
			     cache:false,  
			     dataType:'xml',  
			     success:function(data) {  
			         if(data.msg =="true" ){  
			             alert("修改成功！");  
//			             window.location.reload();  
			         }else{  
			        	 alert(data.msg);  
			         }  
			      },  
			      error : function() {  
			           alert("异常！");  
			      }  
			 });
			 
	}
	
	
	
})(jQuery);