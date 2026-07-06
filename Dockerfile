FROM node:18-slim

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .

# Default command - can be overridden in docker-compose
CMD ["npm", "run", "start:api"]
