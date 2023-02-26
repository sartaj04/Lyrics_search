from django.urls import path
from . import views

urlpatterns = [
    path("", views.home, name="search-engines"),
    path("filters/", views.filters, name="apply-filters"),
]