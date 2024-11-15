<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
	<meta charser="UTF-8">
	<title>no title</title>
	<body>
		Wikimedia recent change producer:

		<br>
		<button id="produceId">Start producing</button>
		<div id="response"></div>

		<script>
		    document.getElementById("produceId").addEventListener("click",function(){

		        const eventSource = new EventSource("startProduce");

		        eventSource.onmessage = function(event) {
		            const eventOutput = document.getElementById("response");
		            eventOutput.innerHTML += "<p>" + event.data + "</p>";
		        }

		        eventSource.onerror = function() {
                                eventSource.close();
                                eventOutput.innerHTML += "<p>Event stream closed.</p>";
                            };

		    });
		</script>
	</body
</html>