# disasterprofile-backend
Backend services and script to transform various datasoruces and produce data used by disasterprofile frontend

# start api backend with pm2

cd api && pm2 start ts-node --name api -- --type-check server.ts --watch
