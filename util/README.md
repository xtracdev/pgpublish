aws ecs register-task-definition --cli-input-json file://$PWD/taskdef.json

aws ecs run-task --cluster blue --task-definition genagg