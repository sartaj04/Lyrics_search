from django.db import models

# Create your models here.
class Songs(models.Model):
	title = models.CharField(max_length=100)
	artist = models.CharField(max_length=100)
	lyrics = models.TextField()
	date_released = models.DateTimeField()