# teacher-effectiveness

## Instructions to setup your local environment

 1. Add your own config.ini with `ACCESS_KEY` and `SECRET_KEY` (use config_template.ini) for reference.
 
 2. Open terminal and change directory to the base of `teacher-effectiveness`.
 
 3. Build the docker container with the command `docker build -t teacher-effectiveness-app:v1 .`.
 
 4. Launch the container with the command `docker run -p 5000:5000 teacher-effectiveness-app:v1`.
 
 5. Open the url `localhost:5000` on the browser of your local machine. You should find the flask app running. 
 
 6. Docker logs can be found in the terminal.

 7. Alternatively, you can run it using docker compose by running (in the teacher effectiveness directory):
 `docker-compose build` then `docker-compose up`, after which it will be accessible on `localhost:5000`.
 This approach also allows you to edit files locally in the editor/IDE of your choice and automatically reloads as you make changes.

