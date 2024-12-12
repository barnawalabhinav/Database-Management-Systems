#!/bin/bash

for ($i=1; $i -lt 11; $i++) {
    Write-Host "EASY_$i"
    Measure-Command { psql -h localhost -U postgres -d health_db -p 5432 -o "OUTPUT/EASY_$i.txt" -f "2020CS50415/EASY/EASY_$i.sql" }
}

for ($i=1; $i -lt 11; $i++) {
    Write-Host "INTERMEDIATE_$i"
    Measure-Command { psql -h localhost -U postgres -d health_db -p 5432 -o "OUTPUT/INTERMEDIATE_$i.txt" -f "2020CS50415/INTERMEDIATE/INTERMEDIATE_$i.sql" }
}

for ($i=1; $i -lt 7; $i++) {
    Write-Host "HARD_$i"
    Measure-Command { psql -h localhost -U postgres -d health_db -p 5432 -o "OUTPUT/HARD_$i.txt" -f "2020CS50415/HARD/HARD_$i.sql" }
}
