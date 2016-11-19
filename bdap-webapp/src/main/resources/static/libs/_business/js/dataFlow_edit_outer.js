/**
 * 针对外围鼠标按下事件
 */
var mouseOuterActionDown = function() {
	var o = document.getElementById("temp");

	o.style.display = "block";
	o.style.left = "0px";
	o.style.top =  "0px";

	booleanoutermove = true;
}
