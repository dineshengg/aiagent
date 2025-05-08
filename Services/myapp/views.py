from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse, JsonResponse
from django.template import loader
import json


def index(request):
    template = loader.get_template("myapp/index.html")
    return HttpResponse(template.render(None, request))

def myapp(request):
    
    data = {
        "grafanaurl": "http://localhost:3000/grafana",
        "error": "success",
    }
    
    if request.method == "POST":
        try:
            # Parse the JSON payload from the request body
            req = json.loads(request.body)
            prompt = req.get("prompt", "")  # Extract the 'prompt' key from the payload
            data["grafanaurl"] = data["grafanaurl"] + prompt
            return JsonResponse(data)
        except json.JSONDecodeError:
            # Handle JSON parsing errors
            return JsonResponse({"error": "Invalid JSON payload"}, status=400)
    else:
        # Return an error for non-POST requests
        return JsonResponse({"error": "Invalid request method"}, status=405)
