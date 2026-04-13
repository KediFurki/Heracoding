var radius = 10;
	var size = 1;
	var color ="#b51c29";
	var style="solid";
	$.fn.connect = function(to){
		alert("");
		var el1, el2;
		el1 = $(this);
		el2 = $(to)
		if (el1.offset().top>el2.offset().top){
			el2 = el1;
			el1 =  $(to);
		}
		var pa = $(el1).offset();
		var pb = $(el2).offset();
		var wa = $(el1).width();
		var wb = $(el2).width();		
		var ha = $(el1).height();
		var hb = $(el2).height();
		var pTop = $(el1).css("padding-top");
		var pBootom = $(el1).css("padding-bottom");
		var paddingBootom =  parseInt(pBootom.replace("px",""));
		var paddingTop =  parseInt(pTop.replace("px",""));
		ha += paddingTop + paddingBootom;		 
		if (pb.left <= pa.left){
			if (pb.left == pa.left)
				rad = 0;
			else
				rad = radius;			
			console.log("paddingBootom:"+paddingBootom+" paddingTop: "+paddingTop+" pa.left: " + pa.left + " pa.top:" +pa.top+" pb.left:"+pb.left+" pb.top:"+pb.top+" (wa/2):" + (wa/2)+" (wb/2):"+(wb/2) );
			var offset = (pa.left - pb.left) - (wa/2) + (wb/2);
			console.log("offset: " + offset );			
			var leftconnection = pa.left + (wa/2) - offset
			var topconnection = pa.top + ha;						
			var hconnection = (pb.top - pa.top-ha)/2;
			var wconnection = offset;							
			console.log("topconnection:  "+topconnection + " pa.top: "+pa.top + " ha: "+ ha+"  hconnection: " + hconnection+" wconnection:" + offset);			
			var element = document.createElement("connection");						
			$(element).css({ 
			    width: wconnection,
			    height: hconnection,						    
			    position: "absolute",				    
			    top: topconnection, 
			    left: leftconnection + rad,	
			    borderBottomWidth : size,
			    borderBottomColor: color,
				borderBottomStyle: style ,
			    borderRightWidth : size,
			    borderRightColor: color,
				borderRightStyle: style,
			    borderBottomRightRadius : rad, 			 
			    width: wconnection - (rad), 
			    height: hconnection
			}).appendTo('body');
			
			topconnection += hconnection;
			console.log("topconnection: "+topconnection);
			var element = document.createElement("connection");
			$(element).css({ 			    
			    position: "absolute",				    
			    top: topconnection -1, left: leftconnection,
			    borderLeftWidth : size,
			    borderLeftColor: color,
				borderLeftStyle: style,
				borderTopWidth : size,
				borderTopColor: color,
				borderTopStyle: style,
			    borderTopLeftRadius : rad, 
			    width:wconnection - rad,
			    height:hconnection		    
			}).appendTo('body'); 
		}else{
			console.log("pa.left: " + pa.left +" pb.left:"+pb.left+" (wa/2)  " + (wa/2)+" (wb/2):"+(wb/2) );
			var offset = (pb.left - pa.left) - (wa/2) + (wb/2);  ///------
			console.log("offset: " + offset );			
			var leftconnection = pa.left + (wa/2) ///+ offset  ///---------
			var topconnection = pa.top + ha;			
			var hconnection = (pb.top - pa.top-ha)/2;
			var wconnection = offset;				
			console.log("hconnection: " + hconnection+" wconnection:" + offset);
			var element = document.createElement("connection");
			$(element).css({ 
			    position: "absolute",				    
			    top: topconnection, 
			    left: leftconnection,	
			    borderBottomWidth : size,
			    borderBottomColor: color,
				borderBottomStyle: style ,
				borderLeftWidth : size,
			    borderLeftColor: color,
				borderLeftStyle: style,
			    borderBottomLeftRadius : radius, 			 
			    width: wconnection - (radius), 
			    height: hconnection
			}).appendTo('body');
			
			topconnection += hconnection;
			console.log("topconnection: "+topconnection);
			var element = document.createElement("connection");
			$(element).css({ 
			    position: "absolute",				    
			    top: topconnection-1, left: leftconnection + radius,
			    borderRightWidth : size,
			    borderRightColor: color,
				borderRightStyle: style,
				borderTopWidth : size,
				borderTopColor: color,
				borderTopStyle: style,
			    borderTopRightRadius : radius, 
			    width:wconnection - radius,
			    height:hconnection
			}).appendTo('body'); 
		}
	}