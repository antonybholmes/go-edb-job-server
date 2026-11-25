openssl ecparam -name prime256v1 -genkey -noout -out cloudfront.es256.private.pem
openssl ec -in cloudfront.es256.private.pem -pubout -out cloudfront.es256.public.pem
