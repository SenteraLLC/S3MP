aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "475283710372.dkr.ecr.us-east-1.amazonaws.com"
docker-compose build
docker push "475283710372.dkr.ecr.us-east-1.amazonaws.com/s3mp:s3mp-lambda-img"
