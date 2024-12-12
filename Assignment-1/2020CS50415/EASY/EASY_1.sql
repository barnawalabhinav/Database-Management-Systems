select COUNT(*)
from patients
where gender = 'F'
    and anchor_age >= 18
    and anchor_age <= 30;