<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
	<meta charser="UTF-8">
	<title>Consumer</title>
	<body>
		Wikimedia recent change consuming:

		<br>
		<button id="consumerId">Start Consume</button>
		<div id="response"></div>

		<script>
		    document.getElementById("consumerId").addEventListener("click",function(){

		        const eventSource = new EventSource("startConsume");

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