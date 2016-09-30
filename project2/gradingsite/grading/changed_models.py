from __future__ import unicode_literals

from django.db import models

# Create your models here.
import datetime
from django.utils import timezone


class Instructor(models.Model):
	name = models.CharField(max_length=50)
	rank = models.CharField(max_length=50)
	def __str__(self):
		return self.name

class Course(models.Model):
	title = models.CharField(max_length=50)
    	instructor = models.ManyToManyField(Instructor)
	credits = models.IntegerField()
	description = models.CharField(max_length=500)
	def __str__(self):
		return self.title
	
class Section(models.Model):
	course = models.ForeignKey(Course, on_delete=models.CASCADE)
	sec_id = models.IntegerField()
	semester = models.CharField(max_length=20)
	year = models.DateTimeField()
	def __str__(self):
		return "Section #: {}, Semester: {}, Year: {}".format(self.sec_id, self.semester, self.year)

class Student(models.Model):
	name = models.CharField(max_length=50)
	courses = models.ManyToManyField(Course)
	def __str__(self):
		return self.name

class Assignment(models.Model):
	course = models.ForeignKey(Course)
	assignment_no = models.IntegerField()
	def __str__(self):
		return "Course Title: {}, Assignment No.: {}, Due on: {}".format(self.course.title, self.assignment_no, self.due_date)
	
class Question(models.Model):
	qtype = models.ForeignKey(QType)
	assignment = models.ForeignKey(Assignment)
	question_no = models.IntegerField()
	question_text = models.CharField(max_length=500)
	
	
class QType(Models.Model):
	trueorfalse = models.BooleanField()
	mult_choice = models.CharField(max_length=1)
	essay = models.CharField(max_length=500)
	fill_in_blank = models.CharField(max_length=50)
	
class Answer(models.Model):
	question = models.ForeignKey(Question)
	answers = models.CharField(max_length=100)
	score = models.IntegerField()
	submission_no = models.IntegerField()

class StudentAssignment(models.Model):
	assignment = models.ForeignKey(Assignment)
	student = models.ForeignKey(Student)
	due_date = models.DateTimeField()
	

	
