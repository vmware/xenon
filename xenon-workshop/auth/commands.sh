#! /bin/sh
#
#  cURL commands:


prompt () {
  echo ""
  echo ""
  echo $1
  echo "[Press Any Key]"
  read
}

####

prompt "# Login as xenon-admin@vmware.com:"
curl -D - -X POST -H "Content-Type: application/json" -H "Authorization: Basic eGVub24tYWRtaW5Adm13YXJlLmNvbTp4ZW5vbg==" -H "Cache-Control: no-cache" -H "Postman-Token: 96c79456-ced3-4f67-dc21-406205d70c03" -d '{
	requestType=LOGIN
}' "http://localhost:8000/core/authn/basic"


prompt "# Create user foo@bar.com:"
curl -X POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" -H "x-xenon-auth-token: $AUTH_TOKEN" -d '{
	email=foo@bar.com
}' "http://localhost:8000/core/authz/users"


prompt "# Add credentials for foo@bar.com:"
curl -X POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" -H "x-xenon-auth-token: $AUTH_TOKEN" -d '{
	"userEmail"="foo@bar.com", "privateKey"="foo"
}' "http://localhost:8000/core/auth/credentials"


prompt "# List all users in the system:"
curl -X GET -H "Content-Type: application/json" -H "Cache-Control: no-cache" -H "x-xenon-auth-token: $AUTH_TOKEN" "http://localhost:8000/core/authz/users"


prompt "# Create a user group:"
curl -X POST -H "Content-Type: application/json" -H "x-xenon-auth-token: $AUTH_TOKEN" -H "Cache-Control: no-cache" -d '{
	"query":{"occurance":"MUST_OCCUR","term":{"propertyName":"email","matchValue":"foo@bar.com","matchType":"TERM"}}, "documentSelfLink":"user-group"
}' "http://localhost:8000/core/authz/user-groups"


prompt "# Create a resource group:"
curl -X POST -H "Content-Type: application/json" -H "x-xenon-auth-token: $AUTH_TOKEN" -H "Cache-Control: no-cache" -d '{
	"query":{"occurance":"MUST_OCCUR","term":{"propertyName":"documentSelfLink","matchValue":"*","matchType":"WILDCARD"}}, "documentSelfLink":"resource-group"
}' "http://localhost:8000/core/authz/resource-groups"


prompt "# Create a role:"
curl -X POST -H "Content-Type: application/json" -H "x-xenon-auth-token: $AUTH_TOKEN" -H "Cache-Control: no-cache" -d '{
	"userGroupLink":"/core/authz/user-groups/user-group",  "resourceGroupLink":"/core/authz/resource-groups/resource-group", "verbs":["POST","GET"], "policy":"DENY", "documentSelfLink":"role"
}' "http://localhost:8000/core/authz/roles"


prompt "# Login as foo@bar.cm:"
curl -D - -X POST -H "Content-Type: application/json" -H "Authorization: Basic Zm9vQGJhci5jb206Zm9v" -H "Cache-Control: no-cache" -d '{
	"requestType":"LOGIN"
}' "http://localhost:8000/core/authn/basic"


prompt "# Update role for foo@bar.com"
curl -X PUT -H "Content-Type: application/json" -H "x-xenon-auth-token: $AUTH_TOKEN" -H "Cache-Control: no-cache" -d '{
	"userGroupLink":"/core/authz/user-groups/user-group",  "resourceGroupLink":"/core/authz/resource-groups/resource-group", "verbs":["POST","GET"], "policy":"ALLOW"
}' "http://localhost:8000/core/authz/roles/role"
