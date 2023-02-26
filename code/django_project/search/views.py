from django.shortcuts import render
from django.http import HttpResponse
from search.services import query_search
# Create your views here.

def home(request):
	if request.method=="POST":
		searched = query_search(query = request.POST['searched'])

		#searched = request.POST['searched']

		return render(request, "search/search.html",{'searched':searched})
	else:
		return render(request, "search/search.html")	

def filters(request):
	return HttpResponse('<h1>apply filters</h1>')
