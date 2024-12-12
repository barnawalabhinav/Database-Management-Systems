--
-- Name: department; Type: TABLE; Ref: Table 8
--
CREATE TABLE IF NOT EXISTS department (
    dept_id CHAR(3) NOT NULL UNIQUE PRIMARY KEY,
    dept_name VARCHAR(40)
);

--
-- Name: valid_entry; Type: TABLE; Ref: Table 7
--
CREATE TABLE IF NOT EXISTS valid_entry (
    dept_id CHAR(3) REFERENCES department(dept_id) ON UPDATE CASCADE ON DELETE CASCADE,
    entry_year INTEGER NOT NULL,
    seq_number INTEGER NOT NULL
);

--
-- Name: professor; Type: TABLE; Ref: Table 6
--
CREATE TABLE IF NOT EXISTS professor (
    professor_id VARCHAR(10) PRIMARY KEY,
    professor_first_name VARCHAR(40) NOT NULL,
    professor_last_name VARCHAR(40) NOT NULL,
    office_number VARCHAR(20),
    contact_number CHAR(10) NOT NULL,
    start_year INTEGER,
    resign_year INTEGER,
    dept_id CHAR(3) REFERENCES department(dept_id) ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE professor
ADD CHECK (start_year <= resign_year);

--
-- Name: student; Type: TABLE; Ref: Table 2
--
CREATE TABLE IF NOT EXISTS student (
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40),
    student_id CHAR(11) NOT NULL PRIMARY KEY,
    address VARCHAR(100),
    contact_number CHAR(10) NOT NULL UNIQUE,
    email_id VARCHAR(50) UNIQUE,
    tot_credits NUMERIC NOT NULL CHECK (tot_credits >= 0 AND tot_credits <= 60),
    dept_id CHAR(3) REFERENCES department(dept_id) ON UPDATE CASCADE
);

--
-- Name: courses; Type: TABLE; Ref: Table 3
--
CREATE TABLE IF NOT EXISTS courses (
    course_id CHAR(6) NOT NULL CHECK (
        SUBSTRING(
            course_id
            FROM 4 FOR 3
        ) ~ '^[0-9]{3}$'
    ) PRIMARY KEY,
    course_name VARCHAR(20) NOT NULL UNIQUE,
    course_desc TEXT,
    credits NUMERIC NOT NULL CHECK (credits > 0),
    dept_id CHAR(3) REFERENCES department(dept_id) ON UPDATE CASCADE ON DELETE CASCADE
);

--
-- Name: course_offers; Type: TABLE; Ref: Table 5
--
CREATE TABLE IF NOT EXISTS course_offers (
    course_id CHAR(6) REFERENCES courses(course_id) ON UPDATE CASCADE,
    session VARCHAR(9),
    semester INTEGER NOT NULL CHECK (
        semester BETWEEN 1 AND 2
    ),
    professor_id VARCHAR(10) REFERENCES professor(professor_id) ON UPDATE CASCADE ON DELETE CASCADE,
    capacity INTEGER,
    enrollments INTEGER,
    PRIMARY KEY (course_id, session, semester)
);

--
-- Name: student_courses; Type: TABLE; Ref: Table 4
--
CREATE TABLE IF NOT EXISTS student_courses (
    student_id CHAR(11) REFERENCES student(student_id) ON UPDATE CASCADE,
    course_id CHAR(6),
    session VARCHAR(9),
    semester INTEGER CHECK (
        semester BETWEEN 1 AND 2
    ),
    grade NUMERIC NOT NULL CHECK (
        grade >= 0
        AND grade <= 10
    ),
    FOREIGN KEY (course_id, session, semester) REFERENCES course_offers(course_id, session, semester) ON UPDATE CASCADE ON DELETE CASCADE
);

--
-- Name: student_dept_change; Type: TABLE; Ref: Section 2.1.4
--
CREATE TABLE IF NOT EXISTS student_dept_change (
    old_student_id CHAR(11),
    old_dept_id CHAR(3),
    new_dept_id CHAR(3),
    new_student_id CHAR(11),
    FOREIGN KEY (old_dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (new_dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION courses_check_func()
RETURNS TRIGGER AS $$
BEGIN
    IF SUBSTRING(
            NEW.course_id
            FROM 1 FOR 3
        ) <> NEW.dept_id
    THEN RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER courses_check BEFORE
INSERT ON courses FOR EACH ROW
EXECUTE FUNCTION courses_check_func();

---
--- #### QUESTION 2.1.1, 2.1.3: Trigger for student id validation
---
CREATE OR REPLACE FUNCTION validate_student_id_func()
RETURNS TRIGGER AS $$
DECLARE
    stu_entry_year INTEGER;
BEGIN
    stu_entry_year = SUBSTRING(
        NEW.student_id
        FROM 1 FOR 4
    )::INTEGER;
    
    IF stu_entry_year NOT IN (
            SELECT entry_year
            FROM valid_entry
            WHERE dept_id = NEW.dept_id
        )
        OR LENGTH(NEW.student_id) <> 10
        OR SUBSTRING(
            NEW.student_id
            FROM 8 FOR 3
        )::INTEGER <> (
            SELECT seq_number
            FROM valid_entry
            WHERE dept_id = NEW.dept_id
                AND entry_year = stu_entry_year
        )
        OR SUBSTRING(
            NEW.student_id
            FROM 5 FOR 3
        ) <> NEW.dept_id
        OR SUBSTRING(
            NEW.email_id
            FROM POSITION('@' IN NEW.email_id) + 1
        ) <> (NEW.dept_id || '.iitd.ac.in')
        OR SUBSTRING(
            NEW.email_id
            FROM 1 FOR POSITION('@' IN NEW.email_id) - 1
        ) <> NEW.student_id
        THEN RAISE EXCEPTION 'invalid';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER validate_student_id BEFORE
INSERT ON student FOR EACH
ROW EXECUTE FUNCTION validate_student_id_func();

---
--- #### QUESTION 2.1.2: Trigger to update valid_entry table
---
CREATE OR REPLACE FUNCTION update_seq_number_func()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE valid_entry
    SET seq_number = seq_number + 1
    WHERE dept_id = NEW.dept_id
        AND entry_year = SUBSTRING(
                NEW.student_id
                FROM 1 FOR 4
            )::INTEGER;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER update_seq_number AFTER
INSERT ON student FOR EACH ROW
EXECUTE FUNCTION update_seq_number_func();

CREATE OR REPLACE FUNCTION insert_valid_entry_func()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.entry_year IN (
            SELECT entry_year
            FROM valid_entry
            WHERE dept_id = NEW.dept_id
        ) OR NEW.seq_number <> 1
    THEN RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER insert_valid_entry BEFORE
INSERT ON valid_entry FOR EACH ROW
EXECUTE FUNCTION insert_valid_entry_func();

---
--- #### QUESTION 2.1.4: Trigger for dept change
---
CREATE OR REPLACE FUNCTION dept_change_func()
RETURNS TRIGGER AS $$
    DECLARE
        stu_entry_year INTEGER;
        seq_num INTEGER;
        new_student_id CHAR(11);
BEGIN
    IF OLD.dept_id <> NEW.dept_id AND EXISTS(
        SELECT 1
        FROM department
        WHERE dept_id = OLD.dept_id
    ) THEN
        stu_entry_year = SUBSTRING(
            NEW.student_id
            FROM 1 FOR 4
        )::INTEGER;

        SELECT seq_number INTO seq_num
        FROM valid_entry
        WHERE dept_id = NEW.dept_id
            AND entry_year = stu_entry_year;

        new_student_id = stu_entry_year || NEW.dept_id || LPAD(seq_num::text, 3, '0');

        IF TG_WHEN = 'BEFORE' THEN
            IF EXISTS (
                SELECT 1
                FROM student_dept_change sdc
                WHERE sdc.new_student_id = OLD.student_id
            ) THEN RAISE EXCEPTION 'Department can be changed only once';
            ELSIF SUBSTRING(
                OLD.student_id
                FROM 1 FOR 4
            )::INTEGER < 2022 THEN RAISE EXCEPTION 'Entry year must be >= 2022';
            ELSIF NOT EXISTS (
                SELECT 1
                FROM student_courses
                WHERE student_id = OLD.student_id
            ) THEN RAISE EXCEPTION 'Low Grade';
            ELSIF (
                SELECT AVG(grade)
                FROM student_courses
                WHERE student_id = OLD.student_id
            ) <= 8.5 THEN RAISE EXCEPTION 'Low Grade';
            END IF;

        ELSIF TG_WHEN = 'AFTER' THEN
            UPDATE student
            SET student_id = new_student_id,
                email_id = new_student_id || '@' || NEW.dept_id || '.iitd.ac.in'
            WHERE student_id = OLD.student_id;

            INSERT INTO student_dept_change (old_student_id, old_dept_id, new_dept_id, new_student_id)
            VALUES (
                    OLD.student_id,
                    OLD.dept_id,
                    NEW.dept_id,
                    new_student_id
                );

            UPDATE valid_entry
            SET seq_number = seq_number + 1
            WHERE dept_id = NEW.dept_id
                AND entry_year = SUBSTRING(
                        NEW.student_id
                        FROM 1 FOR 4
                    )::INTEGER;

        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER log_student_dept_change BEFORE
UPDATE ON student FOR EACH ROW
EXECUTE FUNCTION dept_change_func();

CREATE OR REPLACE TRIGGER log_student_dept_change_after AFTER
UPDATE ON student FOR EACH ROW
EXECUTE FUNCTION dept_change_func();

---
--- #### QUESTION 2.2.1: View for course_eval
---
CREATE MATERIALIZED VIEW IF NOT EXISTS course_eval AS
SELECT
    course_id,
    session,
    semester,
    COUNT(student_id) AS number_of_students,
    AVG(grade) AS average_grade,
    MAX(grade) AS max_grade,
    MIN(grade) AS min_grade
FROM
    student_courses
GROUP BY
    course_id, session, semester;

-- Create a trigger to refresh the materialized view on INSERT, UPDATE, or DELETE
CREATE OR REPLACE FUNCTION refresh_course_eval_view()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW course_eval;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Attach the trigger to the underlying table
CREATE OR REPLACE TRIGGER trigger_refresh_course_eval_view
AFTER INSERT OR UPDATE OR DELETE
ON student_courses
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_course_eval_view();

---
--- #### QUESTION 2.2.2: Trigger for total credits
---
CREATE OR REPLACE FUNCTION update_tot_cred_func()
RETURNS TRIGGER AS $$
DECLARE
    credit NUMERIC;
BEGIN
    SELECT credits INTO credit
    FROM courses
    WHERE course_id = NEW.course_id;

    UPDATE student
    SET tot_credits = tot_credits + credit
    WHERE student_id = NEW.student_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER update_tot_cred AFTER
INSERT ON student_courses FOR EACH ROW
EXECUTE FUNCTION update_tot_cred_func();

---
--- #### QUESTION 2.2.3: Trigger for total credits
---
CREATE OR REPLACE FUNCTION check_creds_func()
RETURNS TRIGGER AS $$
DECLARE
    curr_credit NUMERIC;
BEGIN
    SELECT credits INTO curr_credit
    FROM courses
    WHERE course_id = NEW.course_id;
    
    IF (
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT course_id
            FROM student_courses
            WHERE student_id = NEW.student_id
            AND session = NEW.session
            AND semester = NEW.semester
        ) AS curr_courses
    ) >= 5 OR (
        SELECT tot_credits
        FROM student
        WHERE student_id = NEW.student_id
    ) + curr_credit > 60 THEN RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_creds BEFORE
INSERT ON student_courses FOR EACH ROW
EXECUTE FUNCTION check_creds_func();

---
--- #### QUESTION 2.2.4: Trigger for first year course
---
CREATE OR REPLACE FUNCTION first_year_course_func()
RETURNS TRIGGER AS $$
BEGIN
    IF (
        SELECT credits
        FROM courses
        WHERE course_id = NEW.course_id
    ) = 5 AND SUBSTRING(NEW.student_id FROM 1 FOR 4)::INTEGER <>
        SUBSTRING(NEW.session FROM 1 FOR 4)::INTEGER
    THEN RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER first_year_course BEFORE
INSERT ON student_courses FOR EACH ROW
EXECUTE FUNCTION first_year_course_func();

---
--- #### QUESTION 2.2.5: View for course_eval
---
CREATE MATERIALIZED VIEW IF NOT EXISTS student_semester_summary AS
SELECT
    student_id,
    session,
    semester,
    SUM(grade * credits) / SUM(credits) AS sgpa,
    SUM(credits) AS credits
FROM (
    SELECT
        sc.student_id,
        sc.session,
        sc.semester,
        sc.grade,
        c.credits
    FROM
        student_courses sc
    INNER JOIN
        courses c ON c.course_id = sc.course_id
            AND sc.grade >= 5
) AS grade_card
GROUP BY
    student_id, session, semester;

-- Create a trigger to refresh the materialized view on INSERT, UPDATE, or DELETE
CREATE OR REPLACE FUNCTION refresh_student_semester_summary_view()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW student_semester_summary;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Attach the trigger to the underlying table
CREATE OR REPLACE TRIGGER trigger_refresh_student_semester_summary_view
AFTER INSERT OR UPDATE OR DELETE
ON student_courses
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_student_semester_summary_view();

-- Create a trigger to refresh the materialized view on INSERT, UPDATE, or DELETE
CREATE OR REPLACE FUNCTION refresh_student_credit()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE student
    SET tot_credits = tot_credits - (
        SELECT credits
        FROM courses
        WHERE course_id = OLD.course_id
    )
    WHERE student_id = OLD.student_id;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Attach the trigger to the underlying table
CREATE OR REPLACE TRIGGER trigger_refresh_student_credit
AFTER DELETE
ON student_courses
FOR EACH ROW
EXECUTE FUNCTION refresh_student_credit();

CREATE OR REPLACE FUNCTION check26_func()
RETURNS TRIGGER AS $$
BEGIN
    IF (
        SELECT SUM(credits)
        FROM courses
        WHERE course_id IN (
            SELECT course_id
            FROM student_courses
            WHERE student_id = NEW.student_id
                AND session = NEW.session
                AND semester = NEW.semester
        )
    ) + (
        SELECT credits
        FROM courses
        WHERE course_id = NEW.course_id
    ) > 26
    THEN RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check26 BEFORE
INSERT ON student_courses FOR EACH ROW
EXECUTE FUNCTION check26_func();

---
--- #### QUESTION 2.2.6: Trigger for course capacity check
---
CREATE OR REPLACE FUNCTION enroll_cap_func()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM course_offers
        WHERE course_id = NEW.course_id
            AND session = NEW.session
            AND semester = NEW.semester
            AND enrollments = capacity
    ) THEN RAISE EXCEPTION 'course is full';
    END IF;

    -- IF TG_WHEN = 'AFTER' THEN
        UPDATE course_offers
        SET enrollments = enrollments + 1
        WHERE course_id = NEW.course_id
            AND session = NEW.session
            AND semester = NEW.semester;
    -- END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER enroll_cap BEFORE
INSERT ON student_courses FOR EACH ROW
EXECUTE FUNCTION enroll_cap_func();

CREATE OR REPLACE FUNCTION decr_enroll_func()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE course_offers
    SET enrollments = enrollments - 1
    WHERE course_id = OLD.course_id
        AND session = OLD.session
        AND semester = OLD.semester;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER decr_enroll AFTER
DELETE ON student_courses FOR EACH ROW
EXECUTE FUNCTION decr_enroll_func();

---
--- #### QUESTION 2.3.1: Trigger for course_offers table
---
CREATE OR REPLACE FUNCTION modify_course_offer_func()
RETURNS TRIGGER AS $$
DECLARE
    credit NUMERIC;
BEGIN
    IF TG_OP = 'DELETE' THEN
        SELECT credits INTO credit
        FROM courses
        WHERE course_id = OLD.course_id;

        UPDATE student
        SET tot_credits = tot_credits - credit
        WHERE student_id IN (
            SELECT student_id
            FROM student_courses sc
            WHERE sc.course_id = OLD.course_id
                AND sc.semester = OLD.semester
                AND sc.session = OLD.session
        );

        RETURN OLD;
    ELSIF TG_OP = 'INSERT' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM courses
            WHERE course_id = NEW.course_id
        ) OR NOT EXISTS (
            SELECT 1
            FROM professor
            WHERE professor_id = NEW.professor_id
        ) THEN
            RAISE EXCEPTION 'invalid';
        END IF;

        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER modify_course_offer BEFORE
INSERT OR DELETE ON course_offers FOR EACH ROW
EXECUTE FUNCTION modify_course_offer_func();

---
--- #### QUESTION 2.3.2: Trigger for course_offers table
---
CREATE OR REPLACE FUNCTION insert_course_offer_func()
RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT COUNT(*)
        FROM course_offers
        WHERE professor_id = NEW.professor_id
            AND session = NEW.session
    ) > 3 OR (SELECT resign_year
        FROM professor
        WHERE professor_id = NEW.professor_id
    ) < SUBSTRING(NEW.session FROM 6 FOR 4)::INTEGER
    THEN RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER insert_course_offer BEFORE
INSERT ON course_offers FOR EACH ROW
EXECUTE FUNCTION insert_course_offer_func();

---
--- #### QUESTION 2.4.1: Trigger for department table
---
CREATE OR REPLACE FUNCTION modify_dept_func()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_WHEN = 'BEFORE' AND TG_OP = 'DELETE' THEN
        IF EXISTS (
            SELECT 1
            FROM student
            WHERE dept_id = OLD.dept_id
        ) THEN
            RAISE EXCEPTION 'Department has students';
        END IF;

        DELETE FROM course_offers
        WHERE SUBSTRING(course_id FROM 1 FOR 3) = OLD.dept_id;

        -- DELETE FROM courses
        -- WHERE dept_id = OLD.dept_id;
        
        -- DELETE FROM professor
        -- WHERE dept_id = OLD.dept_id;

        -- DELETE FROM valid_entry
        -- WHERE dept_id = OLD.dept_id;

        RETURN OLD;
    ELSIF TG_WHEN = 'AFTER' AND TG_OP = 'UPDATE' THEN
        IF NEW.dept_id <> OLD.dept_id
        THEN
            UPDATE student
            SET dept_id = NEW.dept_id,
                student_id = SUBSTRING(student_id FROM 1 FOR 4) || NEW.dept_id || SUBSTRING(student_id FROM 8 FOR 3),
                email_id = student_id || NEW.dept_id || SUBSTRING(email_id FROM POSITION('@' IN email_id)+4)
            WHERE dept_id = NEW.dept_id;

            UPDATE student_dept_change
            SET new_student_id = SUBSTRING(new_student_id FROM 1 FOR 4) || new_dept_id || SUBSTRING(new_student_id FROM 8 FOR 3),
                old_student_id = SUBSTRING(old_student_id FROM 1 FOR 4) || old_dept_id || SUBSTRING(old_student_id FROM 8 FOR 3)
            WHERE new_dept_id = NEW.dept_id
                OR old_dept_id = NEW.dept_id;

            UPDATE courses
            SET course_id = NEW.dept_id || SUBSTRING(course_id FROM 4 FOR 3)
            WHERE dept_id = NEW.dept_id;

        END IF;

        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER modify_dept_bef BEFORE
DELETE ON department FOR EACH ROW
EXECUTE FUNCTION modify_dept_func();

CREATE OR REPLACE TRIGGER modify_dept_aft AFTER
UPDATE ON department FOR EACH ROW
EXECUTE FUNCTION modify_dept_func();
