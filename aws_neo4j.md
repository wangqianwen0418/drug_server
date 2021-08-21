**new instance**  
`aws ec2 run-instances   --image-id ami-0e7aaceb2011fa475   --count 1   --instance-type t3.medium   --key-name $KEY_NAME   --security-groups $GROUP   --query "Instances[*].InstanceId"   --region us-east-2`

**add new users**
`CREATE USER reader SET PASSWORD "reader_password" SET PASSWORD CHANGE NOT REQUIRED`
`GRANT ROLE reader TO reader`
**show all users**
`SHOW USERS`