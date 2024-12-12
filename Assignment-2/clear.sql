DELETE FROM department;
DELETE FROM valid_entry;
DELETE FROM professor;
DELETE FROM courses;
DELETE FROM course_offers;
DELETE FROM student;
DELETE FROM student_courses;

DROP TABLE department cascade;
DROP TABLE professor cascade;
DROP TABLE student cascade;
DROP TABLE courses cascade;
DROP TABLE course_offers cascade;
DROP TABLE student_courses cascade;
drop table valid_entry cascade;
drop table student_dept_change cascade;