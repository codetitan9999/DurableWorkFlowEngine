FROM node:22-alpine

WORKDIR /app

COPY apps/web/package.json ./
COPY apps/web/tsconfig.json ./
COPY apps/web/tsconfig.node.json ./
COPY apps/web/vite.config.ts ./
COPY apps/web/index.html ./

RUN npm install

COPY apps/web/src ./src

EXPOSE 5173

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]

