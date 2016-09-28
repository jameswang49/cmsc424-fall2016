from django.shortcuts import render, get_object_or_404

from django.http import HttpResponse, HttpResponseRedirect
from grading.models import Instructor, Course, Assignment, Student, StudentAssignment
from django.urls import reverse
import datetime
from django.utils import timezone


# Create your views here.

def mainindex(request):
        context = { 'instructor_list': Instructor.objects.all() }
        return render(request, 'grading/index.html', context)

def instructorindex(request, instructor_id):
	i = get_object_or_404(Instructor, pk=instructor_id)
	c_list = i.course_set.all()
	count_arr = []
	count_sum = 0
	
	for c in c_list:
		count_sum = 0
		for a in c.assignment_set.all():
			count_sum =  count_sum + a.studentassignment_set.count()
			
		count_arr.append(count_sum)
	
	
	course_count_arr = zip(c_list, count_arr)
	
        context = { 'instructor_id': instructor_id, 'course_list': i.course_set.all(), 'course_count_arr': course_count_arr }
        return render(request, 'grading/instructorindex.html', context)

def instructorcourse(request, instructor_id, course_id):
	c = get_object_or_404(Course, pk=course_id)
	a_list = c.assignment_set.filter(due_date__gte=timezone.now())
	p_list = c.assignment_set.filter(due_date__lte=timezone.now())
	context = { 'instructor_id': instructor_id, 'course_id': course_id, 'course_title': c.title, 'active_assignment_list': a_list, 'past_assignment_list': p_list }
        return render(request, 'grading/instructorcourse.html', context)

def instructorassignment(request, instructor_id, course_id, assignment_id):
	# Should get a list of all submissions for this assignment, and set it in context
	today = timezone.now()
	course = Course.objects.get(pk=course_id)
	assignment = Assignment.objects.get(pk=assignment_id)
	
	sorted_students = Course.objects.get(pk=course_id).student_set.distinct().order_by('name')
	sa_list = Assignment.objects.get(pk=assignment_id).studentassignment_set.all()
	submitted_list = []
	not_submitted_list = []
	
	if assignment.due_date >= today:
		for s in sorted_students:
			for sa in sa_list:
				if sa.student.id == s.id:
					submitted_list.append(s.name)
				else:
					not_submitted_list.append(s.name)
					
	submitted = submitted_list.sort()
	not_submitted = not_submitted_list.sort()
					  
	context = { 'instructor_id': instructor_id, 'course_id': course_id, 'assignment_id': assignment_id, 'sa_list': sa_list, 'sorted_students': sorted_students, 'submitted': submited, 'not_submitted': not_submitted, 'course': course, 'assignment': assignment, 'today': today }
        return render(request, 'grading/instructorassignment.html', context)

def instructorcreate(request, instructor_id, course_id):
        context = { 'course_list': Instructor.objects.all() }
        return render(request, 'grading/instructorcreate.html', context)

def instructorgradesubmission(request, instructor_id, course_id, assignment_id, student_id):
	student_obj = Student.objects.get(pk=student_id)
	sa_list = Assignment.objects.get(pk=assignment_id).studentassignment_set.filter(student__id = student_id)
	context = {'student_obj': student_obj, 'sa_list': sa_list}
        return render(request, 'grading/instructorgradesubmission.html', context)

def studentindex(request, student_id):
	today = timezone.now()
	context = { 'student_id': student_id, 'course_list': Student.objects.get(pk=student_id).courses.all(), 'sa_list': Student.objects.get(pk=student_id).studentassignment_set.all(), 'today': today }
	return render(request, 'grading/studentindex.html', context)

def studentassignment(request, student_id, assignment_id):
	context = { 'assignment': Assignment.objects.get(pk=assignment_id), 'student': Student.objects.get(pk=student_id) }
        return render(request, 'grading/studentassignment.html', context)

def submitassignment(request, student_id, assignment_id):
	print request.POST
	answers = " ".join([request.POST["answer{}".format(i)] for i in range(1, 101) if "answer{}".format(i) in request.POST])
	sa = StudentAssignment(student=Student.objects.get(pk=student_id), assignment=Assignment.objects.get(pk=assignment_id), answers=answers, score=-1)
	sa.save()
	return HttpResponseRedirect(reverse('submittedassignment', args=(student_id,assignment_id,)))

def submittedassignment(request, student_id, assignment_id):
	context = { 'student_id': student_id, 'course_list': Student.objects.get(pk=student_id).courses.all() }
	return render(request, 'grading/studentindex.html', context)
