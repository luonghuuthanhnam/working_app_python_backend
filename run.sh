cd /home/ubuntu/working_app_python_backend/app
conda activate py39
uvicorn main:app --host 0.0.0.0 > console_log.txt & disown