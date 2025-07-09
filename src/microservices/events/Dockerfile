FROM --platform=$BUILDPLATFORM node:18-alpine

WORKDIR /usr/src/app

COPY package*.json ./

RUN npm install --production

COPY . .

EXPOSE 8082

CMD ["node", "events.js"]