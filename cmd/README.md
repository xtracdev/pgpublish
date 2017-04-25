Run it on the command line, or build the container and 
run it like 

<pre>
docker run -e DB_USER=$DB_USER --link mypg:postgres -e DB_PASSWORD=$DB_PASSWORD \
-e DB_PORT=$DB_PORT -e DB_NAME=$DB_NAME -e DB_HOST=postgres \
-e http_proxy=$http_proxy \
-e TOPIC_ARN=$TOPIC_ARN \
-e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
-e AWS_REGION=$AWS_REGION xtracdev/pgpublish
</pre>
