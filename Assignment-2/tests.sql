
-- INSERT INTO department(dept_id, dept_name) VALUES ('CSE', 'Computer Science and Engineering');

-- INSERT INTO valid_entry(dept_id, entry_year, seq_number) VALUES ('CSE', 2020, 1);

-- INSERT INTO professor(professor_id, professor_first_name, professor_last_name, office_number, contact_number, start_year, resign_year, dept_id) VALUES ('jindal', 'Rajat', 'Jindal', 'A-101', '1111111111', 2010, 2024, 'CSE');

-- INSERT INTO courses(course_id, course_name, course_desc, credits, dept_id) VALUES ('CSE101', 'Intro. to Prog.', 'Introduction to programming using Python', 4, 'CSE');

-- INSERT INTO course_offers(course_id, session, semester, professor_id, capacity, enrollments) VALUES ('CSE101', '2020-2021', 1, 'jindal', 10, 0);

-- INSERT INTO student(first_name, last_name, student_id, address, contact_number, email_id, tot_credits, dept_id) VALUES ('John', 'Doe', '2020CSE001', 'Delhi', '9999999999', '2020CSE001@CSE.iitd.ac.in', 10, 'CSE');

-- INSERT INTO student_courses(student_id, course_id, session, semester, grade) VALUES ('2020CSE001', 'CSE101', '2020-2021', 1, 8.5);



-- ******** COPILOT GENERATED SQL TESTCASES ******** --

SELECT '----------------------------------------' AS Separator;


INSERT INTO department(dept_id, dept_name) VALUES ('CSE', 'Computer Science and Engineering');
INSERT INTO department(dept_id, dept_name) VALUES ('ECE', 'Electronics n Communication Engineering');
INSERT INTO department(dept_id, dept_name) VALUES ('CSE', 'Electrical Engineering'); -- This should fail due to duplicate dept_id

SELECT '----------------------------------------' AS Separator;


INSERT INTO valid_entry(dept_id, entry_year, seq_number) VALUES ('CSE', 2022, 1);
INSERT INTO valid_entry(dept_id, entry_year, seq_number) VALUES ('ECE', 2022, 1);
INSERT INTO valid_entry(dept_id, entry_year, seq_number) VALUES ('CSE', 2022, 2); -- This should fail due to duplicate dept_id and entry_year
INSERT INTO valid_entry(dept_id, entry_year, seq_number) VALUES ('CSE', 2020, 2); -- This should fail due to duplicate dept_id and entry_year
INSERT INTO valid_entry(dept_id, entry_year, seq_number) VALUES ('EEE', 2022, 1); -- This should fail due to non-existing dept_id

SELECT '----------------------------------------' AS Separator;


INSERT INTO professor(professor_id, professor_first_name, professor_last_name, office_number, contact_number, start_year, resign_year, dept_id) VALUES ('P001', 'John', 'Doe', 'A101', '1234567890', 2020, 2025, 'CSE');
INSERT INTO professor(professor_id, professor_first_name, professor_last_name, office_number, contact_number, start_year, resign_year, dept_id) VALUES ('P002', 'Jane', 'Smith', 'B201', '9876543210', 2018, 2023, 'ECE');
INSERT INTO professor(professor_id, professor_first_name, professor_last_name, office_number, contact_number, start_year, resign_year, dept_id) VALUES ('P003', 'Bob', 'Brown', 'C301', '1112223334', 2019, 2024, 'EEE'); -- This should fail due to non-existing dept_id

SELECT '----------------------------------------' AS Separator;


INSERT INTO student(first_name, last_name, student_id, address, contact_number, email_id, tot_credits, dept_id) VALUES ('Alice', 'Johnson', '2022CSE001', '123 Main St', '1111111111', '2022CSE001@CSE.iitd.ac.in', 30, 'CSE');
INSERT INTO student(first_name, last_name, student_id, address, contact_number, email_id, tot_credits, dept_id) VALUES ('Bob', 'Smith', '2022ECE001', '456 Elm St', '2222222222', '2022ECE001@ECE.iitd.ac.in', 15, 'ECE');
INSERT INTO student(first_name, last_name, student_id, address, contact_number, email_id, tot_credits, dept_id) VALUES ('Charlie', 'Brown', '2022EEE001', '789 Pine St', '3333333333', '2022EEE001@EEE.iitd.ac.in', 10, 'ECE'); -- This should fail due to non-existing dept_id

SELECT '----------------------------------------' AS Separator;


INSERT INTO courses(course_id, course_name, course_desc, credits, dept_id) VALUES ('CSE101', 'Intro to Comp Sci.', '...', 3, 'CSE');
INSERT INTO courses(course_id, course_name, course_desc, credits, dept_id) VALUES ('ECE201', 'Digital Electronics', '...', 4, 'ECE');
INSERT INTO courses(course_id, course_name, course_desc, credits, dept_id) VALUES ('EEE301', 'Power Systems', '...', 5, 'EEE'); -- This should fail due to invalid dept_id

SELECT '----------------------------------------' AS Separator;


INSERT INTO course_offers(course_id, session, semester, professor_id, capacity, enrollments) VALUES ('CSE101', '2022-2023', 1, 'P001', 50, 30);
INSERT INTO course_offers(course_id, session, semester, professor_id, capacity, enrollments) VALUES ('ECE201', '2022-2023', 2, 'P002', 40, 20);
INSERT INTO course_offers(course_id, session, semester, professor_id, capacity, enrollments) VALUES ('EEE301', '2022-2023', 1, 'P003', 60, 30); -- This should fail due to non-existing course_id or professor_id

SELECT '----------------------------------------' AS Separator;


INSERT INTO student_courses(student_id, course_id, session, semester, grade) VALUES ('2022CSE001', 'CSE101', '2022-2023', 1, 9);
INSERT INTO student_courses(student_id, course_id, session, semester, grade) VALUES ('2022ECE001', 'ECE201', '2022-2023', 2, 8);
INSERT INTO student_courses(student_id, course_id, session, semester, grade) VALUES ('2022EEE001', 'EEE301', '2022-2023', 1, 7); -- This should fail due to non-existing course or capacity reached

SELECT '----------------------------------------' AS Separator;


INSERT INTO student(first_name, last_name, student_id, address, contact_number, email_id, tot_credits, dept_id) VALUES ('David', 'Miller', '2022CSE002', '1010 Binary St', '4444444444', '2022CSE002@CSE.iitd.ac.in', 12, 'CSE'); -- This should succeed
INSERT INTO student(first_name, last_name, student_id, address, contact_number, email_id, tot_credits, dept_id) VALUES ('Eva', 'Green', '2022EEE002', '2020 Vision St', '5555555555', '2022EEE002@CSE.iitd.ac.in', 8, 'EEE'); -- This should fail due to invalid student_id, dept_id, or email_id

UPDATE student SET dept_id = 'ECE' WHERE student_id = '2022CSE002'; -- This should fail due to low grade
UPDATE student SET dept_id = 'ECE' WHERE student_id = '2022CSE001'; -- This should succeed

INSERT INTO valid_entry(dept_id, entry_year, seq_number) VALUES ('CSE', 2020, 2); -- This should fail
INSERT INTO valid_entry(dept_id, entry_year, seq_number) VALUES ('CSE', 2020, 1); -- This should succeed
INSERT INTO student(first_name, last_name, student_id, address, contact_number, email_id, tot_credits, dept_id) VALUES ('David', 'Miller', '2020CSE001', '1010 Binary St', '4444444424', '2020CSE001@CSE.iitd.ac.in', 5, 'CSE'); -- This should succeed
UPDATE student SET dept_id = 'ECE' WHERE student_id = '2020CSE001'; -- This should fail due to entry year

UPDATE department SET dept_id = 'COL' WHERE dept_id = 'CSE';

DELETE FROM course_offers WHERE course_id = 'CSE101' AND session = '2022-2023' AND semester = 1;
DELETE FROM course_offers WHERE course_id = 'COL101' AND session = '2022-2023' AND semester = 1;

INSERT INTO course_offers (course_id, session, semester, professor_id, capacity, enrollments) VALUES ('ECE201', '2022-2023', 1, 'P002', 50, 30);
INSERT INTO course_offers (course_id, session, semester, professor_id, capacity, enrollments) VALUES ('ECE201', '2022-2023', 2, 'P002', 50, 30);

UPDATE student_courses SET grade = 9 WHERE student_id = '2022ECE001' AND course_id = 'CSE101' AND session = '2022-2023' AND semester = 1;

INSERT INTO course_offers (course_id, session, semester, professor_id, capacity, enrollments) VALUES ('COL101', '2022-2023', 1, 'P002', 50, 30);

INSERT INTO student_courses (student_id, course_id, session, semester, grade) VALUES ('2022ECE001', 'COL101', '2022-2023', 1, 9);

INSERT INTO courses (course_id, course_name, course_desc, credits, dept_id) VALUES ('ECE101', 'Intro to digit elec.', '...', 5, 'ECE');

INSERT INTO courses (course_id, course_name, course_desc, credits, dept_id) VALUES ('PYL101', 'Intro to physics.', '...', 5, 'PYL'); -- This should fail due to invalid dept_id

INSERT INTO courses (course_id, course_name, course_desc, credits, dept_id) VALUES ('ECE103', 'Intro to digit 1', '...', 5, 'ECE');
INSERT INTO courses (course_id, course_name, course_desc, credits, dept_id) VALUES ('ECE104', 'Intro to digit 2', '...', 5, 'ECE');
INSERT INTO courses (course_id, course_name, course_desc, credits, dept_id) VALUES ('ECE105', 'Intro to digit 3', '...', 5, 'ECE');

INSERT INTO course_offers (course_id, session, semester, professor_id, capacity, enrollments) VALUES ('ECE101', '2022-2023', 1, 'P002', 20, 10);
INSERT INTO course_offers (course_id, session, semester, professor_id, capacity, enrollments) VALUES ('ECE103', '2022-2023', 1, 'P001', 20, 10);
INSERT INTO course_offers (course_id, session, semester, professor_id, capacity, enrollments) VALUES ('ECE104', '2022-2023', 1, 'P001', 20, 10);
INSERT INTO course_offers (course_id, session, semester, professor_id, capacity, enrollments) VALUES ('ECE105', '2022-2023', 1, 'P001', 20, 10);

INSERT INTO course_offers (course_id, session, semester, professor_id, capacity, enrollments) VALUES ('ECE105', '2022-2023', 2, 'P001', 2, 1);

INSERT INTO student_courses (student_id, course_id, session, semester, grade) VALUES ('2022COL002', 'ECE105', '2022-2023', 2, 9);

DELETE from student_courses where course_id = 'COL101';
delete from student_courses where course_id = 'ECE105';
delete from student where dept_id = 'COL';

SELECT '' AS Separator;

