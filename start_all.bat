@echo off
echo =========================================================================
echo Starting National Weather Big Data Analytics Platform (Django Edition)
echo =========================================================================

echo 1. Applying Django Migrations and Seeding Initial Meteorological Telemetry...
cd backend
python manage.py migrate
python seed_data.py
cd ..

echo 2. Launching Django REST Backend on http://localhost:8000 ...
start "Weather Django Backend" cmd /k "cd backend && python manage.py runserver 0.0.0.0:8000"

timeout /t 3 /nobreak >nul

echo 3. Launching React / Vite Command Center on http://localhost:5173 ...
cd frontend
start "Weather Command Center" cmd /k "npm run dev"

echo =========================================================================
echo All services launched!
echo Dashboard:    http://localhost:5173
echo Django API:   http://localhost:8000/api/v1/
echo Django Admin: http://localhost:8000/admin/
echo =========================================================================
